# src/adapters/postgres_repo.py
import json
from datetime import datetime
from typing import List
from dateutil.relativedelta import relativedelta

# psycopg バージョン3を使用
import psycopg
from psycopg.types.json import Jsonb
from psycopg import sql

from src.domain.models import SignInLog, BatchStatus
from src.domain.exceptions import DatabaseError
from src.application.interfaces.db_port import DatabasePort


class PostgresRepository(DatabasePort):
    """PostgreSQL 18 を使用したデータベース操作の実装 (Adapter)"""

    def __init__(self, connection_string: str):
        self.conn_str = connection_string

    def get_batch_status(self, process_name: str) -> BatchStatus:
        query = "SELECT process_name, last_scanned_at FROM batch_status WHERE process_name = %s"
        try:
            with psycopg.connect(self.conn_str) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (process_name,))
                    row = cur.fetchone()
                    if row:
                        return BatchStatus(process_name=row[0], last_scanned_at=row[1])
                    else:
                        # 初回実行時用のデフォルト（例: 現在時刻の1日前）
                        default_time = datetime.utcnow() - relativedelta(days=1)
                        return BatchStatus(
                            process_name=process_name, last_scanned_at=default_time
                        )
        except Exception as e:
            raise DatabaseError(f"バッチステータスの取得に失敗しました: {e}")

    def update_batch_status(self, status: BatchStatus) -> None:
        query = """
            INSERT INTO batch_status (process_name, last_scanned_at)
            VALUES (%s, %s)
            ON CONFLICT (process_name) DO UPDATE 
            SET last_scanned_at = EXCLUDED.last_scanned_at;
        """
        try:
            with psycopg.connect(self.conn_str) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (status.process_name, status.last_scanned_at))
        except Exception as e:
            raise DatabaseError(f"バッチステータスの更新に失敗しました: {e}")

    def ensure_partition_exists(self, target_date: datetime) -> None:
        """翌月分のパーティションを自動作成する"""
        # 例: 2026年4月のデータを入れるパーティション -> entra_signin_logs_y2026m04
        start_of_month = target_date.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        end_of_month = start_of_month + relativedelta(months=1)

        partition_name = f"entra_signin_logs_y{start_of_month.strftime('%Y')}m{start_of_month.strftime('%m')}"

        query = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {partition_table} 
            PARTITION OF entra_signin_logs 
            FOR VALUES FROM ({start}) TO ({end});
        """
        ).format(
            partition_table=sql.Identifier(partition_name),
            start=sql.Literal(start_of_month),
            end=sql.Literal(end_of_month),
        )

        try:
            with psycopg.connect(self.conn_str, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
        except Exception as e:
            raise DatabaseError(
                f"パーティション {partition_name} の作成に失敗しました: {e}"
            )

    def bulk_insert_logs(self, logs: List[SignInLog]) -> int:
        """COPYコマンドとテンポラリテーブルを用いた高速UPSERT"""
        if not logs:
            return 0

        try:
            with psycopg.connect(self.conn_str) as conn:
                with conn.cursor() as cur:
                    # 1. テンポラリテーブルの作成 (トランザクション終了時に自動削除)
                    cur.execute(
                        "CREATE TEMP TABLE tmp_logs (LIKE entra_signin_logs INCLUDING ALL) ON COMMIT DROP;"
                    )

                    # 2. メモリ上のデータをCOPYで一括転送
                    # PostgreSQL 18対応の psycopg3 では cursor.copy() を使用
                    copy_query = """
                        COPY tmp_logs (
                            id, created_at, user_principal_name, display_name, 
                            app_display_name, ip_address, status_success, failure_reason, raw_data
                        ) FROM STDIN
                    """
                    with cur.copy(copy_query) as copy:
                        for log in logs:
                            copy.write_row(
                                (
                                    log.id,
                                    log.created_at,
                                    log.user_principal_name,
                                    log.display_name,
                                    log.app_display_name,
                                    log.ip_address,
                                    log.status_success,
                                    log.failure_reason,
                                    json.dumps(log.raw_data),  # JSONB型用に文字列化
                                )
                            )

                    # 3. テンポラリテーブルから本テーブルへ重複排除（UPSERT）しながら流し込む
                    insert_query = """
                        INSERT INTO entra_signin_logs (
                            id, created_at, user_principal_name, display_name, 
                            app_display_name, ip_address, status_success, failure_reason, raw_data
                        )
                        SELECT 
                            id, created_at, user_principal_name, display_name, 
                            app_display_name, ip_address, status_success, failure_reason, raw_data
                        FROM tmp_logs
                        ON CONFLICT (id, created_at) DO NOTHING;
                    """
                    cur.execute(insert_query)

                    # 実際に挿入された件数を取得
                    inserted_count = cur.rowcount

                # トランザクションのコミット
                conn.commit()
                return inserted_count

        except Exception as e:
            raise DatabaseError(
                f"サインインログの一括登録中にエラーが発生しました: {e}"
            )
