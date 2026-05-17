from fastapi.testclient import TestClient

from adapter.fastapi.app import create_app

client = TestClient(create_app())


def test_health_check_returns_200_and_expected_payload() -> None:
    response = client.get('/api/v1/health')

    assert response.status_code == 200
    assert response.json() == {'status': 'ok', 'service': 'cdp-core'}
