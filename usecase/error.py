class UseCaseError(Exception):
    """Base application error raised by cdp-core use cases"""


class UseCaseNotFoundError(UseCaseError):
    """Raised when an application lookup cannot find a requested resource"""


class UseCaseValidationError(UseCaseError):
    """Raised when service input is not valid for the use case"""


class UseCaseConflictError(UseCaseError):
    """Raised when a service operation conflicts with existing facts"""


class UseCaseDependencyError(UseCaseError):
    """Raised when an outer dependency cannot complete required work"""


class ProfileNotFoundUseCaseError(UseCaseNotFoundError):
    """Raised when a requested customer profile is absent"""


class SegmentNotFoundUseCaseError(UseCaseNotFoundError):
    """Raised when a requested static segment is absent"""


class ExportJobNotFoundUseCaseError(UseCaseNotFoundError):
    """Raised when a requested export job is absent"""


class SegmentDisabledUseCaseError(UseCaseConflictError):
    """Raised when an export targets a disabled segment"""


class InvalidEventUseCaseError(UseCaseValidationError):
    """Raised when an inbound event cannot be accepted by processing"""


class DuplicateEventUseCaseError(UseCaseConflictError):
    """Raised when an event was already processed or recorded"""


class IdentityConflictUseCaseError(UseCaseConflictError):
    """Raised when known identifiers resolve to different profiles"""


class PurchaseConflictUseCaseError(UseCaseConflictError):
    """Raised when source and order_id conflict with a previous purchase"""


class ExportDeliveryUseCaseError(UseCaseDependencyError):
    """Raised when segment export delivery fails at the outbound boundary"""
