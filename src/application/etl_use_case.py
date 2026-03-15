# src/application/etl_use_case.py
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from typing import List

from src.application.interfaces.api_port import GraphApiPort
from src.application.interfaces.db_port import DatabasePort
from src.domain.models import BatchStatus
from src.infrastructure.logger import get_logger

logger = get_logger(__name__)


class EtlUseCase:
    """ETL (Extract, Transform, Load) の一連のフローを制御するユースケース"""

    def __init__(self, api_port: GraphApiPort, db_port: DatabasePort):
        self.api = api_port
        self.db = db_port
        self.process_name = "entra_signin_sync"
        # 一括ロード(COPY)を行うチャンクサイズ
        self.chunk_size = 5000

    def execute(self) -> None:
        logger.info("--- バッチ処理を開始します ---")

        # 1. バッチ状態の取得 (Extract の準備)
        status = self.db.get_batch_status(self.process_name)

        # 反映遅延対策: 前回取得時刻の 5分前 を起点とする
        start_time = status.last_scanned_at - relativedelta(minutes=5)
        logger.info(f"取得開始時刻 (UTC): {start_time}")

        # 2. 翌月パーティションの存在保証
        next_month = datetime.now(timezone.utc) + relativedelta(months=1)
        self.db.ensure_partition_exists(next_month)

        # 3. データ抽出とロード (Extract & Load)
        total_inserted = 0
        latest_log_time = start_time
        buffer: List = []

        # api_port はジェネレータを返すため、1件ずつメモリに優しく処理できる
        for log in self.api.fetch_signin_logs(start_time):
            buffer.append(log)

            # 最新のログ時刻を記録
            if log.created_at > latest_log_time.replace(tzinfo=timezone.utc):
                latest_log_time = log.created_at

            # チャンクサイズに達したらDBへ一括登録 (COPY実行)
            if len(buffer) >= self.chunk_size:
                inserted = self.db.bulk_insert_logs(buffer)
                total_inserted += inserted
                logger.info(
                    f"{len(buffer)} 件を処理し、{inserted} 件を新規登録しました。"
                )
                buffer.clear()

        # 残りのバッファを登録
        if buffer:
            inserted = self.db.bulk_insert_logs(buffer)
            total_inserted += inserted
            logger.info(f"{len(buffer)} 件を処理し、{inserted} 件を新規登録しました。")

        # 4. バッチ状態の更新
        # 今回取得した中で一番新しいログの時刻を次回の起点とする
        new_status = BatchStatus(
            process_name=self.process_name, last_scanned_at=latest_log_time
        )
        self.db.update_batch_status(new_status)

        logger.info(
            f"--- バッチ処理完了: 合計 {total_inserted} 件の新規ログを登録しました ---"
        )
