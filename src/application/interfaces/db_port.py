# src/application/interfaces/db_port.py
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List
from src.domain.models import SignInLog, BatchStatus


class DatabasePort(ABC):
    """データベース操作のインターフェース (Port)"""

    @abstractmethod
    def get_batch_status(self, process_name: str) -> BatchStatus:
        """バッチの実行状態（前回取得時刻など）を取得する"""
        pass

    @abstractmethod
    def update_batch_status(self, status: BatchStatus) -> None:
        """バッチの実行状態を更新する"""
        pass

    @abstractmethod
    def ensure_partition_exists(self, target_date: datetime) -> None:
        """指定された日時のデータが格納できるパーティションが存在することを保証（なければ作成）する"""
        pass

    @abstractmethod
    def bulk_insert_logs(self, logs: List[SignInLog]) -> int:
        """サインインログを高速に一括登録（COPY & UPSERT）する。登録件数を返す"""
        pass
