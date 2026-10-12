# Databricks notebook source
# Databricks notebook source
# =====================================================
# 1️⃣ Widgets
# =====================================================

import json
from pyspark.sql.functions import current_timestamp

start_time = spark.sql("SELECT current_timestamp()").collect()[0][0]

dbutils.widgets.text(
    "table_metadata",
    "{'table_id': '1', 'table_name': 'customers', 'source_system': 'sqlserver', "
    "'source_schema': 'banking', 'source_table': 'customers', "
    "'source_path': '', 'bronze_schema': 'bronze', "
    "'silver_schema': 'silver', 'active_flag': 'True', "
    "'load_order': '1', 'created_at': '2026-02-18 13:23:37.053711'}"
)

dbutils.widgets.text(
    "table_parameters",
    "{'load_type': 'MERGE', "
    "'primary_key': 'customer_id', "
    "'watermark_column': 'updated_at'}"
)

dbutils.widgets.text(
    "run_id",
    "472519629468310"
)
run_id=dbutils.widgets.get("run_id")

# Parse JSON safely
table_metadata = json.loads(dbutils.widgets.get("table_metadata").replace("'", '"'))
table_parameters = json.loads(dbutils.widgets.get("table_parameters").replace("'", '"'))

print("Table Metadata:", table_metadata)
print("Table Parameters:", table_parameters)
print(f"Run ID: {run_id}")

# COMMAND ----------

# COMMAND ----------

# =====================================================
# 2️⃣ Extract Variables
# =====================================================

table_id = int(table_metadata["table_id"])
table_name = table_metadata["table_name"]
source_system = table_metadata["source_system"].lower()
source_schema = table_metadata["source_schema"]
source_table = table_metadata["source_table"]
source_path = table_metadata["source_path"]
bronze_schema = table_metadata["bronze_schema"]

load_type = table_parameters.get("load_type")
watermark_column = table_parameters.get("watermark_column")
#--Creates new table 'bronze'
bronze_table_fqn = f"banking.{bronze_schema}.{table_name}"

print(f"Target Bronze Table: {bronze_table_fqn}")

# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,Metadata Entry
# =====================================================
# Make and entry to audit table
# =====================================================
entry_exists = spark.sql(f"""
    SELECT 1
    FROM banking.metadata.pipeline_runs
    WHERE run_id = {run_id} AND table_id = {table_id}
""").count() > 0

if entry_exists:
    spark.sql(f"""
        UPDATE banking.metadata.pipeline_runs
        SET
            layer = 'Silver',
            start_time = TIMESTAMP('{start_time}'),
            end_time = NULL,
            status = 'INPROGRESS',
            number_of_records = NULL,
            error_message = NULL
        WHERE run_id = {run_id} AND table_id = {table_id}
    """)
else:
    spark.sql(f"""
        INSERT INTO banking.metadata.pipeline_runs
        VALUES (
            {run_id},
            {table_id},
            'Silver',
            TIMESTAMP('{start_time}'),
            NULL,  -- end time
            'INPROGRESS',
            NULL, --number of records
            NULL -- error message
        )
    """)



# COMMAND ----------

# COMMAND ----------

# =====================================================
# 3️⃣ Get Last Watermark (For Filtering Only)
# =====================================================

last_watermark = None

if load_type in ["APPEND", "MERGE"] and watermark_column:
    watermark_df = spark.sql(f"""
        SELECT last_watermark_value
        FROM banking.metadata.table_watermarks
        WHERE table_id = {table_id}
    """)
    
    if watermark_df.count() > 0:
        last_watermark = watermark_df.first()["last_watermark_value"]

print("Last Watermark:", last_watermark)
#--output: Last Watermark: 1900-01-01 00:00:00

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists banking.bronze

# COMMAND ----------

# MAGIC %pip install msal

# COMMAND ----------

# =====================================================
# 4️⃣ Read Source
# =====================================================

import msal

def get_sql_access_token(config):
    app = msal.ConfidentialClientApplication(
        client_id=config["client_id"],
        authority=f"https://login.microsoftonline.com/{config['tenant_id']}",
        client_credential=config["client_secret"],
    )
    result = app.acquire_token_for_client(
        scopes=["https://database.windows.net/.default"]
    )
    if "access_token" not in result:
        raise Exception(
            f"Could not get Entra token: {result.get('error')} - "
            f"{result.get('error_description')}"
        )
    return result["access_token"]


try:
    if source_system == "sqlserver":

        # 🔐 Read connection JSON from secret
        secret_json = dbutils.secrets.get(
            scope="banking-scope",
            key="sqlserver-connection-json"
        )

        config = json.loads(secret_json)

        # =====================================================
        # 🔗 Azure SQL JDBC Connection
        # =====================================================

        jdbc_url = (
            f"jdbc:sqlserver://{config['host']}:{config['port']};"
            f"databaseName={config['database']};"
            "encrypt=true;"
            "trustServerCertificate=false;"
            "hostNameInCertificate=*.database.windows.net;"
            "loginTimeout=60"
        )

        # =====================================================
        # 🔐 Microsoft Entra Service Principal (access token)
        # =====================================================

        jdbc_properties = {
            "accessToken": get_sql_access_token(config),
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

        # =====================================================
        # 🔎 Build Query
        # =====================================================

        if load_type in ["APPEND", "MERGE"] and last_watermark:

            query = f"""
            (
                SELECT *
                FROM {source_schema}.{source_table}
                WHERE {watermark_column} > CAST('{last_watermark}' AS datetime2)
            ) AS src
            """

        else:

            query = f"""
            (
                SELECT *
                FROM {source_schema}.{source_table}
            ) AS src
            """

        # =====================================================
        # 📥 Read from Azure SQL
        # =====================================================

        source_df = spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=jdbc_properties
        )

    elif source_system == "blob":

        source_df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option(
                "cloudFiles.schemaLocation",
                f"/Volumes/banking/source/volume/_schema/{table_name}"
            )
            .option("header", "true")
            .load(source_path)
        )

    else:
        raise ValueError("Unsupported source_system")

    # =====================================================
    # 5️⃣ Add insert_timestamp
    # =====================================================

    source_df = source_df.withColumn(
        "insert_timestamp",
        current_timestamp()
    )

    # =====================================================
    # 6️⃣ Write to Bronze (Append Only)
    # =====================================================

    if source_system == "blob":

        (
            source_df.writeStream
            .format("delta")
            .option(
                "checkpointLocation",
                f"/Volumes/banking/source/volume/_checkpoints/{table_name}"
            )
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(bronze_table_fqn)
        )

        records_read = None

    else:

        (
            source_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(bronze_table_fqn)
        )

        records_read = source_df.count()

    print("Source → Bronze Load Completed Successfully.")
    print("Watermark will be updated after Silver load.")

except Exception as e:

    end_time = spark.sql(
        "SELECT current_timestamp()"
    ).collect()[0][0]

    error_message = str(e)

    spark.sql(f"""
        UPDATE banking.metadata.pipeline_runs
        SET
            end_time = TIMESTAMP('{end_time}'),
            status = 'FAILED',
            error_message = {'NULL' if not error_message else "'" + error_message.replace("'", "") + "'"}
        WHERE table_id = {table_id}
          AND run_id = {run_id}
    """)

    raise

# COMMAND ----------

# COMMAND ----------

# =====================================================
# 🩺 Connection diagnostics
# =====================================================

import socket

config = json.loads(dbutils.secrets.get(scope="banking-scope", key="sqlserver-connection-json"))

# Test 1: can Databricks reach the server at all? (firewall)
try:
    socket.create_connection((config["host"], int(config["port"])), timeout=10).close()
    print("✅ Test 1: network OK")
except Exception as e:
    print("❌ Test 1: cannot reach server:", e)

# Test 2: can we get a token?
token = get_sql_access_token(config)
print("✅ Test 2: token OK")

# Test 3: can we log in, and does the table exist?
jdbc_url = (
    f"jdbc:sqlserver://{config['host']}:{config['port']};"
    f"databaseName={config['database']};"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;loginTimeout=60"
)
try:
    test_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"(SELECT SUSER_SNAME() AS connected_as, DB_NAME() AS db, "
              f"OBJECT_ID('{source_schema}.{source_table}') AS table_id_in_sql) AS t",
        properties={"accessToken": token, "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"},
    )
    display(test_df)
except Exception as e:
    print("❌ Test 3 failed:\n", str(e)[:3000])
    

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM banking.bronze.customers;