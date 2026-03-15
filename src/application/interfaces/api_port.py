# src/application/interfaces/api_port.py
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterator
from src.domain.models import SignInLog


class GraphApiPort(ABC):
    """Microsoft Graph API操作のインターフェース (Port)"""

    @abstractmethod
    def fetch_signin_logs(self, start_time: datetime) -> Iterator[SignInLog]:
        """指定時刻以降のサインインログをジェネレータ（ページネーションを隠蔽）として取得する"""
        pass
