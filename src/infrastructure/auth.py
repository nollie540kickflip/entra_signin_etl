# src/infrastructure/auth.py
import msal
from src.infrastructure.config import Config
from src.domain.exceptions import ETLError


class EntraIdAuth:
    """Entra ID からアクセストークンを取得するクラス"""

    def __init__(self, config: Config):
        self.authority = f"https://login.microsoftonline.com/{config.tenant_id}"
        self.client_id = config.client_id
        self.client_secret = config.client_secret
        self.scope = ["https://graph.microsoft.com/.default"]

    def get_access_token(self) -> str:
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=self.authority,
            client_credential=self.client_secret,
        )
        result = app.acquire_token_for_client(scopes=self.scope)
        if "access_token" in result:
            return result["access_token"]
        else:
            error_desc = result.get("error_description", "Unknown error")
            raise ETLError(f"アクセストークンの取得に失敗しました: {error_desc}")
