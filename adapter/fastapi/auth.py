import os

from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader, HTTPBearer
from fastapi.security.http import HTTPAuthorizationCredentials

api_key_header = APIKeyHeader(name='X-Service-Token', auto_error=False)
http_bearer = HTTPBearer(auto_error=False)


def verify_service_token(
    x_token: str | None = Security(api_key_header),
    bearer: HTTPAuthorizationCredentials | None = Security(http_bearer),
) -> None:
    token = x_token or (bearer.credentials if bearer else None)
    if not token:
        raise HTTPException(status_code=401, detail='Missing service token')

    expected_token = os.environ.get('APP_API_TOKEN', 'test-token')
    if token != expected_token:
        raise HTTPException(status_code=403, detail='Invalid service token')
