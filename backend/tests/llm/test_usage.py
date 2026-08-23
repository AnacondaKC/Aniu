"""Provider token accounting normalization tests."""

from backend.llm import Usage
from backend.llm.usage import anthropic_usage, openai_usage


def test_openai_usage_separates_cached_and_reasoning_tokens() -> None:
    usage = openai_usage(
        {
            "prompt_tokens": 100,
            "completion_tokens": 40,
            "total_tokens": 140,
            "prompt_tokens_details": {"cached_tokens": 25},
            "completion_tokens_details": {"reasoning_tokens": 10},
        }
    )

    assert usage == Usage(
        input=75,
        output=40,
        cache_read=25,
        reasoning=10,
        total_tokens=140,
    )


def test_anthropic_usage_keeps_cache_categories_separate() -> None:
    usage = anthropic_usage(
        {
            "input_tokens": 50,
            "output_tokens": 20,
            "cache_read_input_tokens": 30,
            "cache_creation_input_tokens": 10,
        }
    )

    assert usage == Usage(
        input=50,
        output=20,
        cache_read=30,
        cache_write=10,
        total_tokens=110,
    )


def test_usage_normalizers_tolerate_missing_or_malformed_metrics() -> None:
    assert openai_usage(None) == Usage()
    assert openai_usage({"prompt_tokens": "bad"}) == Usage()
    assert anthropic_usage({"output_tokens": -2}) == Usage()
