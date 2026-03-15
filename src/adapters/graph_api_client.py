# src/adapters/graph_api_client.py
import time
import requests
import uuid
from datetime import datetime, timezone
from typing import Iterator, Dict, Any

from src.domain.models import SignInLog
from src.domain.exceptions import APIRateLimitError, ETLError
from src.application.interfaces.api_port import GraphApiPort
from src.infrastructure.logger import get_logger

logger = get_logger(__name__)


class GraphApiClient(GraphApiPort):
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_url = "https://graph.microsoft.com/v1.0/auditLogs/signIns"

    def fetch_signin_logs(self, start_time: datetime) -> Iterator[SignInLog]:
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        # ISO8601形式の文字列に変換 (Zを付与してUTCを明示)
        start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        # フィルタリング条件 ($filter) と 並び替え ($orderby)
        url = f"{self.base_url}?$filter=createdDateTime gt {start_time_str}&$orderby=createdDateTime asc"

        while url:
            response = requests.get(url, headers=headers)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"API Rate Limit到達. {retry_after}秒待機します...")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            for item in data.get("value", []):
                yield self._parse_to_model(item)

            # ページネーション: 次のページのURLを取得
            url = data.get("@odata.nextLink")

    def _parse_to_model(self, item: Dict[str, Any]) -> SignInLog:
        """APIのJSONレスポンスを SignInLog エンティティに変換 (Transform)"""
        # createdDateTime は "2023-10-01T12:34:56Z" のような形式
        created_str = item.get("createdDateTime", "")
        if created_str.endswith("Z"):
            created_str = created_str[:-1] + "+00:00"
        created_at = datetime.fromisoformat(created_str)

        # ステータス情報の抽出
        status_info = item.get("status", {})
        error_code = status_info.get("errorCode", 0)
        status_success = error_code == 0
        failure_reason = (
            status_info.get("failureReason") if not status_success else None
        )

        return SignInLog(
            id=uuid.UUID(item["id"]),
            created_at=created_at,
            user_principal_name=item.get("userPrincipalName"),
            display_name=item.get("userDisplayName"),
            app_display_name=item.get("appDisplayName"),
            ip_address=item.get("ipAddress"),
            status_success=status_success,
            failure_reason=failure_reason,
            raw_data=item,  # 元のJSONをそのまま保存
        )
