"""
DAG: MongoDB (Atlas) -> Parquet -> GCS
"""
import io
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# Connections & Variables
MONGO_CONN_ID = "MongoConnection"
GCP_CONN_ID = "gcp-bucket"
GCS_BUCKET = "estudo-airflow-teste"
GCS_PREFIX = "pasta1/airbnb/listings"

DATABASE = "sample_airbnb"
COLLECTION = "listingsAndReviews"

default_args = {
    "owner": "luiz phelipe m. goncalves",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="dag_mongodb_to_gcs_parquet",
    start_date=datetime(2024, 9, 30),
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
    tags=["mongodb", "gcs", "parquet"],
)
def init():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def extract_and_load(ds=None):
        # 1) Extrai do MongoDB
        hook = MongoHook(conn_id=MONGO_CONN_ID)
        query = {"property_type": "House"}
        # projeção opcional para evitar campos pesados/aninhados problemáticos
        projection = {"reviews": 0}

        cursor = hook.find(
            mongo_db=DATABASE,
            mongo_collection=COLLECTION,
            query=query,
            projection=projection,
        )
        documents = list(cursor)
        if not documents:
            print("Nenhum documento encontrado.")
            return None
        print(f"Documentos extraídos: {len(documents)}")

        # 2) Normaliza para DataFrame
        # _id e tipos Decimal/Date precisam virar string para serializar bem em Parquet
        for d in documents:
            d["_id"] = str(d.get("_id"))
        df = pd.json_normalize(documents, sep="_")

        # Converte colunas object “sujas” (listas/dicts remanescentes) para string
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].astype(str)

        # 3) Serializa em Parquet (em memória)
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
        buffer.seek(0)

        # 4) Upload para GCS particionado por data de execução
        object_name = f"{GCS_PREFIX}/dt={ds}/listings.parquet"
        gcs = GCSHook(gcp_conn_id=GCP_CONN_ID)
        gcs.upload(
            bucket_name=GCS_BUCKET,
            object_name=object_name,
            data=buffer.getvalue(),
            mime_type="application/octet-stream",
        )
        uri = f"gs://{GCS_BUCKET}/{object_name}"
        print(f"Arquivo salvo em {uri} | linhas={len(df)}")
        return uri

    start >> extract_and_load() >> end


dag = init()