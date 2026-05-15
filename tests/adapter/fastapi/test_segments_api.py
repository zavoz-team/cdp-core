from datetime import datetime, timezone
from decimal import Decimal
from typing import cast

from fastapi.testclient import TestClient

from adapter.fastapi.app import create_app
from adapter.fastapi.dependencies import get_stub
from domain.identity import CustomerIdentifiers
from domain.profile import Currency, CustomerProfile
from domain.segment import SegmentDefinition, SegmentId
from usecase.dto import GetSegmentMembersResult, ListSegmentsResult, SegmentSummary
from usecase.error import SegmentNotFoundUseCaseError
from usecase.segment import SegmentService


class MockSegmentService:
    async def list_segments(self, include_disabled: bool = False) -> ListSegmentsResult:
        return ListSegmentsResult(
            segments=(
                SegmentSummary(
                    definition=SegmentDefinition(
                        segment_id=SegmentId.NEW_USER,
                        name='New User',
                        description='New User members',
                        is_active=True,
                    ),
                    members_count=10,
                ),
                SegmentSummary(
                    definition=SegmentDefinition(
                        segment_id=SegmentId.ACTIVE,
                        name='Active',
                        description='Active members',
                        is_active=True,
                    ),
                    members_count=20,
                ),
                SegmentSummary(
                    definition=SegmentDefinition(
                        segment_id=SegmentId.VIP,
                        name='VIP',
                        description='VIP members',
                        is_active=True,
                    ),
                    members_count=100,
                ),
            )
        )

    async def get_segment_members(
        self, segment_id: SegmentId, limit: int = 50, offset: int = 0
    ) -> GetSegmentMembersResult:
        if segment_id != SegmentId.VIP:
            raise SegmentNotFoundUseCaseError(f'segment not found: {segment_id}')

        profile = CustomerProfile(
            customer_id='cust_1',
            first_seen_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            last_seen_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
            created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            updated_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
            identifiers=CustomerIdentifiers(emails=('test@example.com',)),
            orders_count=1,
            total_revenue=Decimal('100.50'),
            currency=Currency.RUB,
            current_segments=frozenset([SegmentId.VIP]),
        )

        return GetSegmentMembersResult(
            segment=SegmentDefinition(
                segment_id=SegmentId.VIP,
                name='VIP',
                description='VIP members',
                is_active=True,
            ),
            members=(profile,),
            total=1,
            limit=limit,
            offset=offset,
        )


def override_segment_service() -> SegmentService:
    return cast(SegmentService, MockSegmentService())


app = create_app()
app.dependency_overrides[get_stub(SegmentService)] = override_segment_service
client = TestClient(app)


def test_list_segments_returns_200() -> None:
    response = client.get('/api/v1/segments')

    assert response.status_code == 200
    data = response.json()
    items = data['items']
    assert len(items) == 3
    assert items[0]['segment_id'] == 'new_user'
    assert items[1]['segment_id'] == 'active'
    assert items[2]['segment_id'] == 'vip'
    assert items[2]['name'] == 'VIP'
    assert items[2]['members_count'] == 100


def test_get_segment_members_returns_200() -> None:
    response = client.get('/api/v1/segments/vip/members?limit=10&offset=0')

    assert response.status_code == 200
    data = response.json()

    assert data['segment_id'] == 'vip'
    assert data['total'] == 1
    assert data['limit'] == 10
    assert data['offset'] == 0

    items = data['items']
    assert len(items) == 1
    assert items[0]['customer_id'] == 'cust_1'
    assert items[0]['email'] == 'test@example.com'
    assert items[0]['total_orders'] == 1
    assert items[0]['total_revenue'] == '100.50'
    assert items[0]['segments'] == ['vip']


def test_get_segment_members_returns_404_for_unknown() -> None:
    response = client.get('/api/v1/segments/unknown/members')

    assert response.status_code == 404
    assert response.json()['detail'] == 'Segment not found'


def test_get_segment_members_returns_404_for_not_found_but_valid_enum() -> None:
    response = client.get('/api/v1/segments/active/members')

    assert response.status_code == 404
    assert response.json()['detail'] == 'Segment not found'
