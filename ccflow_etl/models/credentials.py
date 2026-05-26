import os
from typing import Dict, List, Optional

from ccflow import BaseModel
from pydantic import Field

__all__ = (
    "Credentials",
    "NoCredentials",
    "UsernamePasswordCredentials",
    "APITokenCredentials",
    "APIKeySecretCredentials",
    "OAuthCredentials",
)


def _env_value(name: Optional[str]) -> Optional[str]:
    return os.environ.get(name) if name else None


class Credentials(BaseModel):
    name: Optional[str] = None

    def is_configured(self) -> bool:
        return False


class NoCredentials(Credentials): ...


class UsernamePasswordCredentials(Credentials):
    username: Optional[str] = None
    password: Optional[str] = Field(default=None, repr=False)
    username_env: Optional[str] = None
    password_env: Optional[str] = None

    def resolved_username(self) -> Optional[str]:
        return self.username or _env_value(self.username_env)

    def resolved_password(self) -> Optional[str]:
        return self.password or _env_value(self.password_env)

    def is_configured(self) -> bool:
        return self.resolved_username() is not None and self.resolved_password() is not None


class APITokenCredentials(Credentials):
    token: Optional[str] = Field(default=None, repr=False)
    token_env: Optional[str] = None
    scheme: str = "Bearer"

    def resolved_token(self) -> Optional[str]:
        return self.token or _env_value(self.token_env)

    def is_configured(self) -> bool:
        return self.resolved_token() is not None


class APIKeySecretCredentials(Credentials):
    api_key: Optional[str] = Field(default=None, repr=False)
    secret_key: Optional[str] = Field(default=None, repr=False)
    api_key_env: Optional[str] = None
    secret_key_env: Optional[str] = None

    def resolved_api_key(self) -> Optional[str]:
        return self.api_key or _env_value(self.api_key_env)

    def resolved_secret_key(self) -> Optional[str]:
        return self.secret_key or _env_value(self.secret_key_env)

    def is_configured(self) -> bool:
        return self.resolved_api_key() is not None and self.resolved_secret_key() is not None


class OAuthCredentials(Credentials):
    client_id: Optional[str] = None
    client_secret: Optional[str] = Field(default=None, repr=False)
    client_id_env: Optional[str] = None
    client_secret_env: Optional[str] = None
    token_url: Optional[str] = None
    access_token: Optional[str] = Field(default=None, repr=False)
    refresh_token: Optional[str] = Field(default=None, repr=False)
    scopes: List[str] = Field(default_factory=list)
    extra: Dict[str, str] = Field(default_factory=dict)

    def resolved_client_id(self) -> Optional[str]:
        return self.client_id or _env_value(self.client_id_env)

    def resolved_client_secret(self) -> Optional[str]:
        return self.client_secret or _env_value(self.client_secret_env)

    def is_configured(self) -> bool:
        return self.resolved_client_id() is not None and self.resolved_client_secret() is not None
