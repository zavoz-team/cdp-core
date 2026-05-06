class DomainError(Exception):
    """Base error for cdp-core domain contract violations."""


class InvalidEventError(DomainError):
    """Raised when an event envelope or payload is invalid for the domain."""


class DuplicateEventError(DomainError):
    """Raised when an event_id was already processed."""


class IdentityConflictError(DomainError):
    """Raised when known identifiers resolve to different customers."""


class ProfileNotFoundError(DomainError):
    """Raised when a requested customer profile does not exist."""


class SegmentNotFoundError(DomainError):
    """Raised when a requested static segment does not exist."""


class SegmentDisabledError(DomainError):
    """Raised when activation or membership requires an enabled segment."""


class ExportJobNotFoundError(DomainError):
    """Raised when a requested activation job does not exist."""


class ActivationDeliveryError(DomainError):
    """Raised when an activation delivery attempt is invalid or failed."""


class UnsupportedCurrencyError(DomainError):
    """Raised when a non-RUB currency enters the domain model."""


class PurchaseConflictError(DomainError):
    """Raised when a source and order_id pair has conflicting purchase data."""
