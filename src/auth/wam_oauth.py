# src/auth/wam_oauth.py
from __future__ import annotations
import time, os
from dataclasses import dataclass
from typing import Optional
import requests

WAM_TOKEN_URL = os.getenv("WAM_TOKEN_URL", "https://wam.roche.com/as/token.oauth2")

@dataclass
class OAuthToken:
    access_token: str
    expires_at: float
    def is_valid(self, skew: int = 60) -> bool:
        return time.time() + skew < self.expires_at

class _BaseTM:
    def __init__(self, token_url: str = WAM_TOKEN_URL, timeout: int = 30):
        self.token_url = token_url
        self.timeout = timeout
        self._tok: Optional[OAuthToken] = None
    def get_token(self) -> str:
        if self._tok is None or not self._tok.is_valid():
            self._tok = self._fetch()
        return self._tok.access_token

class WAMPasswordTokenManager(_BaseTM):
    """
    ROPC flow using the same defaults as your working project.
    """
    def __init__(
        self,
        username: str,
        password: str,
        client_id: str = os.getenv("WAM_CLIENT_ID", "snowflake"),
        client_secret: str = os.getenv("WAM_CLIENT_SECRET", "snowflake"),
        scope: str = os.getenv("WAM_SCOPE", "session:role-any"),
        **kw,
    ):
        super().__init__(**kw)
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope

    def _fetch(self) -> OAuthToken:
        data = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
            "scope": self.scope,
            # many IdPs expect client auth via HTTP Basic; still fine to include client_id
            "client_id": self.client_id,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
        r = requests.post(
            self.token_url,
            data=data,
            headers=headers,
            auth=(self.client_id, self.client_secret),  # Basic client auth
            timeout=self.timeout,
        )
        # helpful error detail
        try:
            j = r.json()
        except Exception:
            r.raise_for_status()
            raise
        if r.status_code != 200:
            raise RuntimeError(f"WAM token error {r.status_code}: {j}")
        return OAuthToken(
            access_token=j["access_token"],
            expires_at=time.time() + int(j.get("expires_in", 300)),
        )
