"""OpenAI Chat Completions driver derived from Pi AI.

Upstream inspiration: packages/ai/src/api/openai-completions.ts (MIT).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import urlparse

import httpx

from backend.llm.abort import AbortSignal, await_with_abort, throw_if_aborted
from backend.llm.contracts import (
    AssistantMessage,
    LLMChatMessage,
    ModelProtocol,
    StopReason,
)
from backend.llm.errors import LLMIntegrationError, provider_error
from backend.llm.events import StreamStarted
from backend.llm.provider_config import (
    ModelAuthMode,
    ModelProviderConfig,
    OpenAIMaxTokensField,
)
from backend.llm.providers.context_payload import provider_context_payload
from backend.llm.providers.extract import _extract_openai_chat_text
from backend.llm.providers.http import (
    _auth_headers,
    _resolved_auth_mode,
    _sdk_endpoint,
    _validate_api_key,
)
from backend.llm.providers.openai_stream import OpenAIStreamAccumulator
from backend.llm.providers.types import (
    DriverRequest,
    EventSink,
    ProviderStreamFailure,
)
from backend.llm.retry import RetryPolicy, retry_provider_request


@dataclass(frozen=True, slots=True)
class OpenAICompat:
    max_tokens_field: str = "max_tokens"
    supports_temperature: bool = True
    supports_top_p: bool = True
    supports_stream_usage: bool = True


def _compat(request: DriverRequest) -> OpenAICompat:
    model = request.model.lower()
    host = (urlparse(request.base_url).hostname or "").lower()
    official = host == "api.openai.com" or host.endswith(".openai.com")
    reasoning_model = model.startswith(("o1", "o3", "o4", "gpt-5"))
    overrides = request.provider_config.openai
    inferred_max_tokens_field = (
        "max_completion_tokens" if reasoning_model else "max_tokens"
    )
    max_tokens_field = (
        inferred_max_tokens_field
        if overrides.max_tokens_field is OpenAIMaxTokensField.AUTO
        else overrides.max_tokens_field.value
    )
    return OpenAICompat(
        max_tokens_field=max_tokens_field,
        supports_temperature=(
            not reasoning_model
            if overrides.supports_temperature is None
            else overrides.supports_temperature
        ),
        supports_top_p=(
            not reasoning_model
            if overrides.supports_top_p is None
            else overrides.supports_top_p
        ),
        supports_stream_usage=(
            official
            if overrides.supports_stream_usage is None
            else overrides.supports_stream_usage
        ),
    )


class OpenAIChatDriver:
    protocol = ModelProtocol.OPENAI_CHAT_COMPLETIONS

    def _params(self, request: DriverRequest, *, stream: bool) -> dict[str, Any]:
        compat = _compat(request)
        params: dict[str, Any] = {
            "model": request.model,
            **provider_context_payload(
                protocol=request.protocol,
                messages=request.messages,
                tools=request.tools,
                model=request.model,
                provider_config=request.provider_config,
            ),
            "stream": stream,
        }
        params[compat.max_tokens_field] = request.max_output_tokens
        if compat.supports_temperature:
            params["temperature"] = request.temperature
        if compat.supports_top_p:
            params["top_p"] = request.top_p
        if request.thinking_effort is not None:
            params["reasoning_effort"] = request.thinking_effort
        if stream and compat.supports_stream_usage:
            params["stream_options"] = {"include_usage": True}
        return params

    async def stream_message(
        self,
        *,
        client: httpx.AsyncClient,
        request: DriverRequest,
        emit: EventSink,
        abort_signal: AbortSignal | None,
        retry_policy: RetryPolicy,
    ) -> AssistantMessage:
        throw_if_aborted(abort_signal)
        await emit(StreamStarted(request.protocol, request.model))
        sdk_base_url = _sdk_endpoint(
            request.protocol,
            request.base_url,
        )
        key = _validate_api_key(request.api_key)
        auth_mode = _resolved_auth_mode(
            request.protocol, request.base_url, request.provider_config
        )
        default_headers: dict[str, Any] | None = None
        if auth_mode is ModelAuthMode.API_KEY:
            default_headers = {"api-key": key}
        from openai import AsyncOpenAI, omit

        sdk = AsyncOpenAI(
            api_key=key,
            base_url=sdk_base_url,
            default_headers=cast(Any, default_headers),
            http_client=client,
            max_retries=0,
        )
        params = self._params(request, stream=True)
        if auth_mode is ModelAuthMode.API_KEY:
            params["extra_headers"] = {"Authorization": omit}

        async def create_stream() -> Any:
            return await cast(Any, sdk.chat.completions).create(**params)

        stream = await retry_provider_request(
            create_stream,
            signal=abort_signal,
            policy=retry_policy,
        )
        accumulator = OpenAIStreamAccumulator(request.protocol, request.model)
        try:
            iterator = stream.__aiter__()
            while True:
                try:
                    chunk = await await_with_abort(anext(iterator), abort_signal)
                except StopAsyncIteration:
                    break
                await accumulator.accept(chunk.model_dump(), emit)
            throw_if_aborted(abort_signal)
            return await accumulator.finish(emit)
        except Exception as exc:
            aborted = bool(abort_signal is not None and abort_signal.aborted)
            error = exc if aborted else provider_error(exc)
            if isinstance(error, LLMIntegrationError) and accumulator.has_output:
                error.retryable_override = False
            message = await accumulator.failure_message(
                emit,
                stop_reason=StopReason.ABORTED if aborted else StopReason.ERROR,
                error_message=str(error) or type(error).__name__,
            )
            raise ProviderStreamFailure(error, message) from exc
        finally:
            await stream.close()

    def build_text_request(
        self,
        *,
        base_url: str = "",
        api_key: str,
        model: str,
        system_prompt: str | None,
        user_prompt: str,
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        provider_config: ModelProviderConfig = ModelProviderConfig(),
    ) -> dict[str, object]:
        messages: list[LLMChatMessage] = []
        if system_prompt and system_prompt.strip():
            messages.append({"role": "system", "content": system_prompt.strip()})
        messages.append({"role": "user", "content": user_prompt})
        request = DriverRequest(
            protocol=self.protocol,
            base_url=base_url,
            api_key=api_key,
            model=model,
            messages=messages,
            tools=[],
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            provider_config=provider_config,
        )
        return {
            "method": "POST",
            "url": "/chat/completions",
            "headers": {
                **_auth_headers(
                    protocol=self.protocol,
                    base_url=base_url,
                    api_key=api_key,
                    provider_config=provider_config,
                ),
                "Content-Type": "application/json",
            },
            "json": self._params(request, stream=False),
        }

    def parse_text_response(self, payload: dict[str, object]) -> str:
        return _extract_openai_chat_text(cast(dict[str, Any], payload))


__all__ = ["OpenAIChatDriver", "OpenAICompat"]
