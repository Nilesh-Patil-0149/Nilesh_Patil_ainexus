import os
import json
import datetime
import urllib
import pandas as pd
import logging
import argparse

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError

from azure.ai.ml import MLClient, load_job

# ============================================================
# LOGGING CONFIGURATION
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================
# 1. Load Customer Header Mapping from SQL DB
# ============================================================
def load_customer_header_map():
    try:
        with open("config.json") as f:
            cfg = json.load(f)
    except Exception as e:
        logger.error("Failed to load config.json → %s", str(e))
        raise

    try:
        server = cfg["server"]
        database = cfg["database"]
        username = cfg["username"]
        password = cfg["password"]
    except KeyError as e:
        logger.error("Missing SQL config key: %s", str(e))
        raise

    try:
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "Encrypt=yes;TrustServerCertificate=no;"
            "Authentication=SqlPassword"
        )

        connection_str = f"mssql+pyodbc:///?odbc_connect={params}"
        engine = create_engine(connection_str)
        conn = engine.connect()

        df = pd.read_sql(text("SELECT * FROM [dbo].[mapping_header]"), conn)
        df["header_list"] = df["header_list"].apply(
            lambda x: [c.strip() for c in x.split(",")]
        )
        return df

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error: %s", str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error while loading header map: %s", str(e))
        raise


# ============================================================
# 2. Identify Customer
# ============================================================
def identify_customer(df_incoming: pd.DataFrame, schema_df: pd.DataFrame):
    try:
        incoming_cols = {c.lower() for c in df_incoming.columns}

        for _, row in schema_df.iterrows():
            expected_cols = {c.lower() for c in row["header_list"]}
            if incoming_cols == expected_cols:
                return row["customer_name"]

        return None
    except Exception as e:
        logger.error("Customer identification failed: %s", str(e))
        raise


# ============================================================
# 3. Filename Generator
# ============================================================
def generate_new_filename(old_name: str, customer_name: str) -> str:
    try:
        base, ext = os.path.splitext(old_name)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_customer = customer_name.replace(" ", "_")
        return f"{safe_customer}_{base}_{ts}{ext}"
    except Exception as e:
        logger.error("Filename generation failed: %s", str(e))
        raise


# ============================================================
# 4. Move file inside ADLS container
# ============================================================
def move_file_to_customer_folder(
    container_name: str,
    src_blob_path: str,
    dest_blob_path: str,
    connection_string: str
):
    try:
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        container = blob_service.get_container_client(container_name)

        source_blob = container.get_blob_client(src_blob_path)
        dest_blob = container.get_blob_client(dest_blob_path)

        dest_blob.start_copy_from_url(source_blob.url)
        source_blob.delete_blob()

        logger.info("File moved successfully → %s", dest_blob_path)

    except AzureError as e:
        logger.error("Azure blob operation failed: %s", str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error moving blob: %s", str(e))
        raise


# ============================================================
# 5. Get Azure ML Client
# ============================================================
def get_ml_client() -> MLClient:
    try:
        credential = DefaultAzureCredential()

        ml_client = MLClient(
            credential=credential,
            subscription_id=os.getenv("AZURE_SUBSCRIPTION_ID"),
            resource_group_name=os.getenv("AZURE_RG"),
            workspace_name=os.getenv("AZURE_ML_WORKSPACE")
        )
        return ml_client
    except Exception as e:
        logger.error("Failed to initialize Azure ML Client: %s", str(e))
        raise


# ============================================================
# 6. Trigger Bronze Pipeline via YAML
# ============================================================
def trigger_azure_ml_pipeline(customer_name: str, bronze_path: str):
    try:
        ml_client = get_ml_client()

        with open("config/pipeline_config.json") as f:
            pipeline_cfg = json.load(f)

        cust_cfg = pipeline_cfg.get(customer_name, pipeline_cfg["default"])
        pipeline_yaml_path = cust_cfg["pipeline_name"]

        logger.info("[PIPELINE] Customer: %s", customer_name)
        logger.info("[PIPELINE] YAML Path: %s", pipeline_yaml_path)

        # Load pipeline job from YAML
        job = load_job(pipeline_yaml_path)

        # These names must match inputs in each customer pipeline.yaml
        job.inputs["customer_name"] = customer_name
        job.inputs["bronze_path"] = bronze_path

        created_job = ml_client.jobs.create_or_update(job)
        logger.info("[OK] Pipeline triggered: %s", created_job.name)

        return created_job

    except AzureError as e:
        logger.error("Azure ML pipeline execution failed: %s", str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error triggering ML pipeline: %s", str(e))
        raise


# ============================================================
# 7. Main Orchestration
# ============================================================
def orchestrate_single_file(
    landing_container: str,
    landing_blob_path: str,
    storage_connection_string: str
):
    try:
        schema_df = load_customer_header_map()

        blob_service = BlobServiceClient.from_connection_string(storage_connection_string)
        container = blob_service.get_container_client(landing_container)
        blob = container.get_blob_client(landing_blob_path)

        df_incoming = pd.read_csv(blob.download_blob())

        customer = identify_customer(df_incoming, schema_df)
        if not customer:
            raise ValueError(
                f"UNKNOWN HEADER → Cannot identify customer for file {landing_blob_path}"
            )

        logger.info("[INFO] Customer Identified → %s", customer)

        old_name = os.path.basename(landing_blob_path)
        new_name = generate_new_filename(old_name, customer)

        safe_cust = customer.replace(" ", "_")
        dest_path = f"customers/{safe_cust}/{new_name}"

        move_file_to_customer_folder(
            container_name=landing_container,
            src_blob_path=landing_blob_path,
            dest_blob_path=dest_path,
            connection_string=storage_connection_string
        )

        # For the pipeline, bronze_path is the dest ADLS path we just created
        trigger_azure_ml_pipeline(customer, dest_path)

        logger.info("[SUCCESS] Orchestration Completed Successfully ✔")

    except Exception as e:
        logger.error("[FAILURE] Orchestration failed: %s", str(e))
        raise


# ============================================================
# 8. ENTRY POINT (for Azure ML command job)
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_name", type=str, required=True)
    args = parser.parse_args()

    file_name = args.file_name
    logger.info("Incoming file name: %s", file_name)

    landing_container = "shippingproject"
    landing_blob_path = f"landing_zone/{file_name}"

    storage_conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    orchestrate_single_file(
        landing_container=landing_container,
        landing_blob_path=landing_blob_path,
        storage_connection_string=storage_conn_str,
    )
