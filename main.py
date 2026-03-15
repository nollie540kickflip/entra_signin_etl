# main.py
import sys
from src.infrastructure.config import load_config
from src.infrastructure.auth import EntraIdAuth
from src.infrastructure.logger import get_logger
from src.adapters.graph_api_client import GraphApiClient
from src.adapters.postgres_repo import PostgresRepository
from src.application.etl_use_case import EtlUseCase

logger = get_logger("main")


def main():
    try:
        # 1. 設定の読み込み
        config = load_config()

        # 2. 認証: Entra ID からアクセストークンを取得
        logger.info("Entra ID のアクセストークンを取得中...")
        auth = EntraIdAuth(config)
        access_token = auth.get_access_token()

        # 3. 依存オブジェクト（アダプター）の生成
        api_client = GraphApiClient(access_token)
        db_repo = PostgresRepository(config.db_conn_str)

        # 4. ユースケースの生成と実行 (Dependency Injection)
        use_case = EtlUseCase(api_port=api_client, db_port=db_repo)
        use_case.execute()

    except Exception as e:
        logger.error(f"予期せぬエラーが発生しました: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
