# src/infrastructure/config.py
import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class Config:
    tenant_id: str
    client_id: str
    client_secret: str
    db_conn_str: str


def load_config() -> Config:
    load_dotenv()  # .env ファイルを読み込む

    tenant_id = os.getenv("TENANT_ID")
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")

    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "entra_db")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "")

    if not all([tenant_id, client_id, client_secret]):
        raise ValueError("必須の環境変数（Entra ID認証情報）が設定されていません。")

    # psycopg3 用の接続文字列
    db_conn_str = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    return Config(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        db_conn_str=db_conn_str,
    )
