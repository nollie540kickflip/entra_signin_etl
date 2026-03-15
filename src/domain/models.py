# src/domain/models.py
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any


@dataclass
class SignInLog:
    """サインインログを表現するエンティティ"""

    id: uuid.UUID
    created_at: datetime  # UTCのdatetimeオブジェクト
    user_principal_name: Optional[str]
    display_name: Optional[str]
    app_display_name: Optional[str]
    ip_address: Optional[str]
    status_success: bool
    failure_reason: Optional[str]
    raw_data: Dict[str, Any]


@dataclass
class BatchStatus:
    """バッチの実行状態を管理するエンティティ"""

    process_name: str
    last_scanned_at: datetime
