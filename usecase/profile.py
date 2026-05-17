from usecase.criteria import ProfileListCriteria
from usecase.dto import GetProfileResult, ListProfilesResult
from usecase.error import ProfileNotFoundUseCaseError
from usecase.interface import CustomerProfileRepository


class ProfileService:
    def __init__(self, profile_repository: CustomerProfileRepository) -> None:
        self._profile_repository = profile_repository

    async def get_profile(self, customer_id: str) -> GetProfileResult:
        profile = await self._profile_repository.get_by_customer_id(customer_id)
        if profile is None:
            raise ProfileNotFoundUseCaseError(f'profile not found: {customer_id}')

        return GetProfileResult(profile=profile)

    async def list_profiles(
        self,
        criteria: ProfileListCriteria,
    ) -> ListProfilesResult:
        profiles = await self._profile_repository.list_profiles(criteria)
        total = await self._profile_repository.count_profiles(criteria)

        return ListProfilesResult(
            profiles=profiles,
            total=total,
            limit=criteria.limit,
            offset=criteria.offset,
        )
