import os

from ccflow import BaseModel
from pydantic import Field

__all__ = (
    "APIKeySecretCredentials",
    "APITokenCredentials",
    "Credentials",
    "NoCredentials",
    "OAuthCredentials",
    "UsernamePasswordCredentials",
)


def _env_value(name: str | None) -> str | None:
    return os.environ.get(name) if name else None


class Credentials(BaseModel):
    name: str | None = None

    def is_configured(self) -> bool:
        return False


class NoCredentials(Credentials): ...


class UsernamePasswordCredentials(Credentials):
    username: str | None = None
    password: str | None = Field(default=None, repr=False)
    username_env: str | None = None
    password_env: str | None = None

    def resolved_username(self) -> str | None:
        return self.username or _env_value(self.username_env)

    def resolved_password(self) -> str | None:
        return self.password or _env_value(self.password_env)

    def is_configured(self) -> bool:
        return self.resolved_username() is not None and self.resolved_password() is not None


class APITokenCredentials(Credentials):
    token: str | None = Field(default=None, repr=False)
    token_env: str | None = None
    scheme: str = "Bearer"

    def resolved_token(self) -> str | None:
        return self.token or _env_value(self.token_env)

    def is_configured(self) -> bool:
        return self.resolved_token() is not None


class APIKeySecretCredentials(Credentials):
    api_key: str | None = Field(default=None, repr=False)
    secret_key: str | None = Field(default=None, repr=False)
    api_key_env: str | None = None
    secret_key_env: str | None = None

    def resolved_api_key(self) -> str | None:
        return self.api_key or _env_value(self.api_key_env)

    def resolved_secret_key(self) -> str | None:
        return self.secret_key or _env_value(self.secret_key_env)

    def is_configured(self) -> bool:
        return self.resolved_api_key() is not None and self.resolved_secret_key() is not None


class OAuthCredentials(Credentials):
    client_id: str | None = None
    client_secret: str | None = Field(default=None, repr=False)
    client_id_env: str | None = None
    client_secret_env: str | None = None
    token_url: str | None = None
    access_token: str | None = Field(default=None, repr=False)
    refresh_token: str | None = Field(default=None, repr=False)
    scopes: list[str] = Field(default_factory=list)
    extra: dict[str, str] = Field(default_factory=dict)

    def resolved_client_id(self) -> str | None:
        return self.client_id or _env_value(self.client_id_env)

    def resolved_client_secret(self) -> str | None:
        return self.client_secret or _env_value(self.client_secret_env)

    def is_configured(self) -> bool:
        return self.resolved_client_id() is not None and self.resolved_client_secret() is not None
