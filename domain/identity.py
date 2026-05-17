from dataclasses import dataclass
from enum import StrEnum

from domain.error import InvalidEventError


class KnownIdentifierType(StrEnum):
    EMAIL = 'email'
    PHONE = 'phone'
    EXTERNAL_USER_ID = 'external_user_id'


class AnonymousIdentifierType(StrEnum):
    ANONYMOUS_ID = 'anonymous_id'


@dataclass(frozen=True, slots=True)
class KnownIdentifier:
    identifier_type: KnownIdentifierType
    value: str

    def __post_init__(self) -> None:
        try:
            identifier_type = KnownIdentifierType(self.identifier_type)
        except ValueError as error:
            raise InvalidEventError(
                f'unsupported known identifier type: {self.identifier_type}'
            ) from error

        if not isinstance(self.value, str) or not self.value.strip():
            raise InvalidEventError('known identifier value must be non-empty')

        object.__setattr__(self, 'identifier_type', identifier_type)


@dataclass(frozen=True, slots=True)
class CustomerIdentifiers:
    emails: tuple[str, ...] = ()
    phones: tuple[str, ...] = ()
    external_user_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, 'emails', _identifier_values(self.emails, 'emails'))
        object.__setattr__(self, 'phones', _identifier_values(self.phones, 'phones'))
        object.__setattr__(
            self,
            'external_user_ids',
            _identifier_values(self.external_user_ids, 'external_user_ids'),
        )

    @property
    def has_known(self) -> bool:
        return bool(self.emails or self.phones or self.external_user_ids)

    def as_known_identifiers(self) -> tuple[KnownIdentifier, ...]:
        identifiers: list[KnownIdentifier] = []

        for value in self.emails:
            identifiers.append(KnownIdentifier(KnownIdentifierType.EMAIL, value))
        for value in self.phones:
            identifiers.append(KnownIdentifier(KnownIdentifierType.PHONE, value))
        for value in self.external_user_ids:
            identifiers.append(
                KnownIdentifier(KnownIdentifierType.EXTERNAL_USER_ID, value)
            )

        return tuple(identifiers)


@dataclass(frozen=True, slots=True)
class IdentityLink:
    identity_type: KnownIdentifierType
    identity_value: str
    customer_id: str

    def __post_init__(self) -> None:
        try:
            identity_type = KnownIdentifierType(self.identity_type)
        except ValueError as error:
            raise InvalidEventError(
                f'IdentityLink supports only known identifiers; got {self.identity_type}'
            ) from error

        if not isinstance(self.identity_value, str) or not self.identity_value.strip():
            raise InvalidEventError('identity_value must be non-empty')
        if not isinstance(self.customer_id, str) or not self.customer_id.strip():
            raise InvalidEventError('customer_id must be non-empty')

        object.__setattr__(self, 'identity_type', identity_type)


def _identifier_values(values: tuple[str, ...], field_name: str) -> tuple[str, ...]:
    if isinstance(values, str):
        raise InvalidEventError(
            f'{field_name} must be a collection of identifier values'
        )

    result = tuple(values)
    for value in result:
        if not isinstance(value, str) or not value.strip():
            raise InvalidEventError(f'{field_name} must contain only non-empty values')

    return result
