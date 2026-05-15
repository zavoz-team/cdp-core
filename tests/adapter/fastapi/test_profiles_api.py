from datetime import datetime, timezone
from decimal import Decimal
from typing import cast

from fastapi.testclient import TestClient

from adapter.fastapi.app import create_app
from adapter.fastapi.dependencies import get_stub
from domain.event import EventType
from domain.identity import CustomerIdentifiers
from domain.profile import Currency, CustomerProfile, RecentEvent
from domain.segment import SegmentId
from usecase.criteria import ProfileListCriteria
from usecase.dto import GetProfileResult, ListProfilesResult
from usecase.error import ProfileNotFoundUseCaseError
from usecase.profile import ProfileService


class MockProfileService:
    async def get_profile(self, customer_id: str) -> GetProfileResult:
        if customer_id == 'not_found':
            raise ProfileNotFoundUseCaseError(f'profile not found: {customer_id}')

        profile = CustomerProfile(
            customer_id=customer_id,
            first_seen_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            last_seen_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
            created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            updated_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
            identifiers=CustomerIdentifiers(
                emails=('test@example.com',),
                phones=('+1234567890',),
                external_user_ids=('ext_123',),
            ),
            attributes={'custom_attr': 'value'},
            last_purchase_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
            events_count=5,
            page_views_count=3,
            cart_adds_count=1,
            orders_count=1,
            total_revenue=Decimal('100.50'),
            currency=Currency.RUB,
            current_segments=frozenset([SegmentId('vip')]),
            recent_events=(
                RecentEvent(
                    event_id='evt_1',
                    event_type=EventType.PURCHASE,
                    occurred_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
                ),
            ),
        )
        return GetProfileResult(profile=profile)

    async def list_profiles(self, criteria: ProfileListCriteria) -> ListProfilesResult:
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
            current_segments=frozenset([SegmentId('vip')]),
        )
        return ListProfilesResult(
            profiles=(profile,),
            total=1,
            limit=criteria.limit,
            offset=criteria.offset,
        )





def override_profile_service() -> ProfileService:
    return cast(ProfileService, MockProfileService())


app = create_app()
app.dependency_overrides[get_stub(ProfileService)] = override_profile_service
client = TestClient(app)


def test_list_profiles_returns_200() -> None:
    response = client.get('/api/v1/profiles?limit=10&offset=0&email=test@example.com')

    assert response.status_code == 200
    data = response.json()
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


def test_get_profile_returns_200() -> None:
    response = client.get('/api/v1/profiles/cust_1')

    assert response.status_code == 200
    data = response.json()

    assert data['customer_id'] == 'cust_1'
    assert data['identifiers']['emails'] == ['test@example.com']
    assert data['counters']['orders_count'] == 1
    assert data['total_revenue'] == '100.50'
    assert data['currency'] == 'RUB'
    assert data['segments'] == ['vip']
    assert len(data['recent_events']) == 1
    assert data['recent_events'][0]['event_id'] == 'evt_1'


def test_get_profile_returns_404_when_not_found() -> None:
    response = client.get('/api/v1/profiles/not_found')

    assert response.status_code == 404
    assert response.json()['detail'] == 'Profile not found'
