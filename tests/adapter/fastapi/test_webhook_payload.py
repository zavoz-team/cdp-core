from datetime import datetime, timezone
from decimal import Decimal
from typing import cast

import httpx
import pytest
from httpx import Response

from adapter.fastapi.webhook_payload import HttpxWebhookGateway, WebhookPayload
from domain.export_job import ActivationDestination, DestinationType
from domain.identity import CustomerIdentifiers
from domain.profile import Currency, CustomerProfile
from domain.segment import SegmentId
from usecase.dto import SegmentExportPayload


def test_webhook_payload_serializes_empty_segment_correctly() -> None:
    dto = SegmentExportPayload(
        job_id='job_empty',
        segment_id=SegmentId.NEW_USER,
        exported_at=datetime(2026, 5, 4, 10, 0, 0, tzinfo=timezone.utc),
        members_count=0,
        members=(),
    )

    payload = WebhookPayload.from_dto(dto)
    json_data = payload.model_dump(mode='json')

    assert json_data == {
        'job_id': 'job_empty',
        'segment_id': 'new_user',
        'exported_at': '2026-05-04T10:00:00Z',
        'members_count': 0,
        'members': [],
    }


def test_webhook_payload_serializes_complex_segment_correctly() -> None:
    profile = CustomerProfile(
        customer_id='cust_001',
        first_seen_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        updated_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        identifiers=CustomerIdentifiers(
            emails=('user@example.com',),
            phones=('+79990000000',),
            external_user_ids=('shop_user_123',),
        ),
        orders_count=3,
        total_revenue=Decimal('125000'),
        currency=Currency.RUB,
        current_segments=frozenset([SegmentId.VIP]),
    )

    dto = SegmentExportPayload(
        job_id='job_001',
        segment_id=SegmentId.VIP,
        exported_at=datetime(2026, 5, 4, 10, 0, 0, tzinfo=timezone.utc),
        members_count=1,
        members=(profile,),
    )

    payload = WebhookPayload.from_dto(dto)
    json_data = payload.model_dump(mode='json')

    assert json_data == {
        'job_id': 'job_001',
        'segment_id': 'vip',
        'exported_at': '2026-05-04T10:00:00Z',
        'members_count': 1,
        'members': [
            {
                'customer_id': 'cust_001',
                'emails': ['user@example.com'],
                'phones': ['+79990000000'],
                'external_user_ids': ['shop_user_123'],
                'segments': ['vip'],
                'stats': {
                    'orders_count': 3,
                    'total_revenue': '125000',
                    'currency': 'RUB',
                },
            }
        ],
    }


class MockAsyncClient(httpx.AsyncClient):
    def __init__(self, response: Response | Exception) -> None:
        super().__init__()
        self._response = response
        self.post_called_with_url: str | None = None
        self.post_called_with_json: dict | None = None

    async def post(self, url: str, **kwargs) -> Response:  # type: ignore
        self.post_called_with_url = url
        self.post_called_with_json = kwargs.get('json')
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


@pytest.mark.anyio
async def test_httpx_gateway_2xx_response_means_success() -> None:
    client = MockAsyncClient(Response(200))
    gateway = HttpxWebhookGateway(client)

    dto = SegmentExportPayload(
        job_id='job_001',
        segment_id=SegmentId.VIP,
        exported_at=datetime(2026, 5, 4, 10, 0, 0, tzinfo=timezone.utc),
        members_count=0,
        members=(),
    )
    destination = ActivationDestination(
        destination_type=DestinationType.WEBHOOK,
        url='https://example.com',
    )

    result = await gateway.send_segment_export(destination, dto)

    assert result.delivered is True
    assert result.response_status_code == 200
    assert result.error_reason is None
    assert client.post_called_with_url == 'https://example.com'
    json_payload = cast(dict, client.post_called_with_json)
    assert json_payload['members'] == []


@pytest.mark.anyio
async def test_httpx_gateway_non_2xx_response_returns_failed_job() -> None:
    client = MockAsyncClient(Response(400, text='Bad Request'))
    gateway = HttpxWebhookGateway(client)

    dto = SegmentExportPayload(
        job_id='job_001',
        segment_id=SegmentId.VIP,
        exported_at=datetime(2026, 5, 4, 10, 0, 0, tzinfo=timezone.utc),
        members_count=0,
        members=(),
    )
    destination = ActivationDestination(
        destination_type=DestinationType.WEBHOOK,
        url='https://example.com',
    )

    result = await gateway.send_segment_export(destination, dto)

    assert result.delivered is False
    assert result.response_status_code == 400
    assert result.error_reason == 'HTTP 400: Bad Request'


@pytest.mark.anyio
async def test_httpx_gateway_timeout_returns_failed_job() -> None:
    client = MockAsyncClient(httpx.TimeoutException('Timeout'))
    gateway = HttpxWebhookGateway(client)

    dto = SegmentExportPayload(
        job_id='job_001',
        segment_id=SegmentId.VIP,
        exported_at=datetime(2026, 5, 4, 10, 0, 0, tzinfo=timezone.utc),
        members_count=0,
        members=(),
    )
    destination = ActivationDestination(
        destination_type=DestinationType.WEBHOOK,
        url='https://example.com',
    )

    result = await gateway.send_segment_export(destination, dto)

    assert result.delivered is False
    assert result.response_status_code is None
    assert result.error_reason == 'Connection timeout'
