from backend.business.shared import (
    IntegrationErrorCode,
    ServiceConfigurationError,
    ServiceIntegrationError,
    classify_http_status,
    is_retryable,
)


def test_classify_http_status_codes() -> None:
    assert classify_http_status(401) is IntegrationErrorCode.AUTHENTICATION
    assert classify_http_status(429) is IntegrationErrorCode.RATE_LIMIT
    assert classify_http_status(503) is IntegrationErrorCode.PROVIDER_5XX
    assert classify_http_status(None) is IntegrationErrorCode.NETWORK


def test_service_integration_error_defaults_code_from_status() -> None:
    err = ServiceIntegrationError("boom", status_code=429)
    assert err.error_code is IntegrationErrorCode.RATE_LIMIT
    assert is_retryable(err.error_code, err.status_code)


def test_context_overflow_is_not_retryable_even_with_transient_status() -> None:
    assert IntegrationErrorCode.CONTEXT_OVERFLOW.value == "context_overflow"
    assert not is_retryable(IntegrationErrorCode.CONTEXT_OVERFLOW, 409)

    err = ServiceConfigurationError("missing key")
    assert err.error_code is IntegrationErrorCode.CONFIGURATION
