"""LLM runtime helpers for generic prompting and controlled tool usage."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from time import perf_counter
from urllib.parse import urlsplit

from backend.agent.errors import (
    AgentConfigurationError,
    AgentErrorCode,
    AgentIntegrationError,
    is_retryable_agent_error,
)
from backend.agent.json import serialize_context
from backend.agent.kernel.abort import throw_if_aborted
from backend.agent.kernel.context import AgentContext
from backend.agent.kernel.context_budget import ContextBudgetConfig
from backend.agent.kernel.runtime_config import LlmRuntimeConfig
from backend.llm import (
    AbortSignal,
    ChatMessage,
    ChatResponse,
    Completed,
    Failed,
    LLMClientPort,
    LLMErrorCode,
    LLMIntegrationError,
    ModelProtocol,
    ModelProviderConfig,
    ReasoningDelta,
    StopReason,
    TextDelta,
    ThinkingEffort,
    ToolDefinition,
    estimate_provider_request_tokens,
    normalize_chat_response,
)
from backend.llm import (
    is_error_retryable as is_retryable_llm_integration,
)

logger = logging.getLogger(__name__)
MAX_LLM_ATTEMPTS = 3
LLM_RETRY_BASE_DELAY_SECONDS = 0.05


ModelCallError = AgentIntegrationError | LLMIntegrationError


def _domain_error_code(exc: ModelCallError) -> AgentErrorCode:
    if isinstance(exc, LLMIntegrationError):
        if exc.error_code is LLMErrorCode.CONTEXT_OVERFLOW:
            return AgentErrorCode.CONTEXT_OVERFLOW
        return AgentErrorCode(exc.error_code.value)
    if exc.error_code is not AgentErrorCode.UNKNOWN:
        return exc.error_code
    status_code = exc.status_code
    if status_code in {401, 403}:
        return AgentErrorCode.AUTHENTICATION
    if status_code == 408:
        return AgentErrorCode.TIMEOUT
    if status_code == 429:
        return AgentErrorCode.RATE_LIMIT
    if status_code is not None and 500 <= status_code <= 599:
        return AgentErrorCode.PROVIDER_5XX
    if status_code is not None and 400 <= status_code <= 499:
        return AgentErrorCode.PROVIDER_4XX
    return AgentErrorCode.UNKNOWN


def _as_service_error(
    exc: ModelCallError,
) -> AgentIntegrationError | AgentConfigurationError:
    if isinstance(exc, AgentIntegrationError):
        return exc
    if exc.error_code is LLMErrorCode.CONFIGURATION:
        return AgentConfigurationError(str(exc), status_code=exc.status_code)
    return AgentIntegrationError(
        str(exc),
        status_code=exc.status_code,
        error_code=_domain_error_code(exc),
    )


def _classify_llm_error(exc: ModelCallError) -> AgentErrorCode:
    if exc.status_code is not None:
        return _domain_error_code(exc)
    message = str(exc).lower()
    authentication_markers = (
        "401",
        "403",
        "unauthorized",
        "forbidden",
        "api key",
        "apikey",
        "authentication",
        "permission",
        "not configured",
    )
    if any(marker in message for marker in authentication_markers):
        return AgentErrorCode.AUTHENTICATION
    if "empty" in message and "content" in message:
        return AgentErrorCode.EMPTY_RESPONSE
    network_markers = (
        "readerror",
        "read error",
        "server disconnected",
        "timeout",
        "timed out",
        "connection",
        "connecterror",
        "network",
    )
    if any(marker in message for marker in network_markers):
        return AgentErrorCode.NETWORK
    if exc.status_code is None and _domain_error_code(exc) is AgentErrorCode.NETWORK:
        # Status-less integration errors are ambiguous. Retry only when the
        # exception contains evidence of a transport failure.
        return AgentErrorCode.UNKNOWN
    return _domain_error_code(exc)


def is_retryable_llm_error(exc: ModelCallError) -> bool:
    if isinstance(exc, LLMIntegrationError):
        return is_retryable_llm_integration(exc)
    code = _classify_llm_error(exc)
    return code is AgentErrorCode.EMPTY_RESPONSE or is_retryable_agent_error(
        code, exc.status_code
    )


def llm_error_code(exc: ModelCallError) -> str:
    if (
        isinstance(exc, LLMIntegrationError)
        and exc.error_code is LLMErrorCode.CONTEXT_OVERFLOW
    ):
        return "LLM_CONTEXT_OVERFLOW"
    code = _classify_llm_error(exc)
    if code in {
        AgentErrorCode.RATE_LIMIT,
        AgentErrorCode.PROVIDER_5XX,
        AgentErrorCode.PROVIDER_4XX,
    } and is_retryable_agent_error(code, exc.status_code):
        return "LLM_HTTP_TRANSIENT"
    if code in {AgentErrorCode.TIMEOUT, AgentErrorCode.NETWORK}:
        return "LLM_NETWORK_ERROR"
    return f"LLM_{code.value.upper()}"


def _retryable_llm_error_code(exc: ModelCallError) -> str | None:
    return llm_error_code(exc) if is_retryable_llm_error(exc) else None


async def _sleep_before_retry(abort_signal: AbortSignal | None, attempt: int) -> None:
    throw_if_aborted(abort_signal)
    await asyncio.sleep(LLM_RETRY_BASE_DELAY_SECONDS * (attempt + 1))
    throw_if_aborted(abort_signal)


def _resolve_runtime(
    context: AgentContext,
    override: LlmRuntimeConfig | None,
) -> LlmRuntimeConfig | None:
    """Prefer an explicit runtime override, otherwise use context.runtime."""

    if override is not None:
        return override
    return context.runtime


def _abort_signal(context: AgentContext) -> AbortSignal | None:
    return context.abort_signal


def _effective_output_cap(
    runtime: LlmRuntimeConfig,
    *,
    messages: list[ChatMessage],
    tools: list[ToolDefinition],
    requested_max_output_tokens: int | None = None,
) -> tuple[int, int]:
    """Return the provider-safe output cap for the exact request context."""

    input_tokens = estimate_provider_request_tokens(
        messages,
        tools,
        protocol=runtime.protocol,
        model=runtime.model,
        provider_config=runtime.provider_config,
    )
    output_tokens = ContextBudgetConfig.from_runtime(runtime).output_tokens_for_input(
        input_tokens,
        requested_max_output_tokens=requested_max_output_tokens,
    )
    return input_tokens, output_tokens


def _text_delta_handler(
    context: AgentContext,
    label: str,
) -> Callable[[str], Awaitable[None]] | None:
    sink = context.stream_sink
    if not callable(sink):
        return None

    async def handle_delta(delta: str) -> None:
        if delta:
            await sink(delta, "text")

    return handle_delta


def _reasoning_delta_handler(
    context: AgentContext,
    label: str,
) -> Callable[[str], Awaitable[None]] | None:
    sink = context.stream_sink
    if not callable(sink):
        return None

    async def handle_delta(delta: str) -> None:
        if delta:
            await sink(delta, "thinking")

    return handle_delta


def _buffering_text_delta_handler(
    buffer: list[str],
) -> Callable[[str], Awaitable[None]]:
    """Collect text deltas without publishing — used during tool-loop turns."""

    async def handle_delta(delta: str) -> None:
        if delta:
            buffer.append(delta)

    return handle_delta


async def _publish_buffered_report_text(
    context: AgentContext,
    label: str,
    text: str,
) -> None:
    """Publish confirmed final-turn text to the UI (no tool_calls on this turn)."""

    sink = context.stream_sink
    if not callable(sink) or not text:
        return
    # Emit in small chunks so the workbench still gets progressive updates
    # without showing provisional text that later disappears.
    chunk_size = 96
    for index in range(0, len(text), chunk_size):
        await sink(text[index : index + chunk_size], "text")


async def _stream_or_chat(
    client: LLMClientPort,
    *,
    protocol: ModelProtocol,
    base_url: str,
    api_key: str,
    model: str,
    messages: list[ChatMessage],
    temperature: float,
    tools: list[ToolDefinition],
    top_p: float,
    max_output_tokens: int,
    thinking_effort: ThinkingEffort | None,
    provider_config: ModelProviderConfig,
    on_text_delta: Callable[[str], Awaitable[None]],
    on_reasoning_delta: Callable[[str], Awaitable[None]] | None,
    abort_signal: AbortSignal | None,
) -> object:
    stream_chat = getattr(client, "stream_chat", None)
    if not callable(stream_chat):
        return await client.chat(
            protocol=protocol,
            base_url=base_url,
            api_key=api_key,
            model=model,
            messages=messages,
            temperature=temperature,
            tools=tools,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            thinking_effort=thinking_effort,
            provider_config=provider_config,
            on_text_delta=on_text_delta,
            on_reasoning_delta=on_reasoning_delta,
            abort_signal=abort_signal,
        )

    response: object = None
    async for event in stream_chat(
        protocol=protocol,
        base_url=base_url,
        api_key=api_key,
        model=model,
        messages=messages,
        temperature=temperature,
        tools=tools,
        top_p=top_p,
        max_output_tokens=max_output_tokens,
        thinking_effort=thinking_effort,
        provider_config=provider_config,
        abort_signal=abort_signal,
    ):
        if isinstance(event, TextDelta):
            await on_text_delta(event.delta)
        elif isinstance(event, ReasoningDelta) and on_reasoning_delta is not None:
            await on_reasoning_delta(event.delta)
        elif isinstance(event, Completed):
            response = event.message
        elif isinstance(event, Failed):
            raise event.error
    return response


def _payload_bytes(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    return len(serialize_context(value).encode("utf-8"))


def _duration_ms(started_at: float) -> int:
    return max(0, int((perf_counter() - started_at) * 1000))


def _provider_name(runtime: LlmRuntimeConfig) -> str:
    try:
        hostname = urlsplit(runtime.base_url).hostname
    except ValueError:
        hostname = None
    return hostname or runtime.protocol.value


def _llm_log_base(
    context: AgentContext,
    *,
    label: str,
    runtime: LlmRuntimeConfig,
    llm_mode: str,
    input_bytes: int,
    max_output_tokens: int | None = None,
) -> dict[str, object]:
    return {
        "agent_label": context.label or label,
        "provider": _provider_name(runtime),
        "protocol": runtime.protocol.value,
        "model": runtime.model,
        "llm_mode": llm_mode,
        "input_bytes": input_bytes,
        "max_output_tokens": (
            runtime.max_output_tokens
            if max_output_tokens is None
            else max_output_tokens
        ),
    }


async def generate_text_output(
    context: AgentContext,
    *,
    label: str,
    user_prompt: str,
    system_prompt: str | None = None,
    llm_runtime: LlmRuntimeConfig | None = None,
    llm_client: LLMClientPort | None = None,
    publish_stream: bool = True,
    max_output_tokens: int | None = None,
) -> str:
    runtime = _resolve_runtime(context, llm_runtime)
    if runtime is None:
        raise AgentConfigurationError(f"{label} requires configured llm runtime")

    if llm_client is None:
        raise AgentConfigurationError(f"{label} llm client is not configured")
    protocol = runtime.protocol
    base_url = runtime.base_url
    api_key = runtime.api_key
    model = runtime.model
    temperature = runtime.temperature
    top_p = runtime.randomness
    generate = getattr(llm_client, "generate_text", None)
    if not callable(generate):
        raise AgentConfigurationError(
            f"{label} llm client does not support text generation"
        )
    client = llm_client
    abort_signal = _abort_signal(context)
    throw_if_aborted(abort_signal)

    request_messages: list[ChatMessage] = []
    normalized_system_prompt = system_prompt.strip() if system_prompt else ""
    if normalized_system_prompt:
        request_messages.append({"role": "system", "content": normalized_system_prompt})
    request_messages.append({"role": "user", "content": user_prompt})
    requested_output_tokens = (
        runtime.max_output_tokens
        if max_output_tokens is None
        else max(1, min(max_output_tokens, runtime.max_output_tokens))
    )
    estimated_input_tokens, effective_output_tokens = _effective_output_cap(
        runtime,
        messages=request_messages,
        tools=[],
        requested_max_output_tokens=requested_output_tokens,
    )

    system_prompt_bytes = _payload_bytes(system_prompt)
    user_prompt_bytes = _payload_bytes(user_prompt)
    log_base = _llm_log_base(
        context,
        label=label,
        runtime=runtime,
        llm_mode="text",
        input_bytes=system_prompt_bytes + user_prompt_bytes,
        max_output_tokens=effective_output_tokens,
    )
    log_base.update(
        {
            "system_prompt_bytes": system_prompt_bytes,
            "user_prompt_bytes": user_prompt_bytes,
            "estimated_input_tokens": estimated_input_tokens,
            "requested_max_output_tokens": requested_output_tokens,
        }
    )

    for attempt in range(MAX_LLM_ATTEMPTS):
        throw_if_aborted(abort_signal)
        started_at = perf_counter()
        try:
            response = await client.generate_text(
                protocol=protocol,
                base_url=base_url,
                api_key=api_key,
                model=model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=float(temperature),
                top_p=float(top_p),
                max_output_tokens=effective_output_tokens,
                thinking_effort=runtime.thinking_effort,
                provider_config=runtime.provider_config,
                on_text_delta=(
                    _text_delta_handler(context, label) if publish_stream else None
                ),
                on_reasoning_delta=(
                    _reasoning_delta_handler(context, label) if publish_stream else None
                ),
                abort_signal=abort_signal,
            )
            duration_ms = _duration_ms(started_at)
            throw_if_aborted(abort_signal)
            logger.info(
                "llm_call_completed",
                extra={
                    **log_base,
                    "attempt": attempt + 1,
                    "max_attempts": MAX_LLM_ATTEMPTS,
                    "retry_count": attempt,
                    "duration_ms": duration_ms,
                    "output_bytes": _payload_bytes(response),
                    "status": "completed",
                },
            )
            return response
        except AgentConfigurationError:
            logger.error(
                "llm_call_failed",
                extra={
                    **log_base,
                    "attempt": attempt + 1,
                    "max_attempts": MAX_LLM_ATTEMPTS,
                    "retry_count": attempt,
                    "duration_ms": _duration_ms(started_at),
                    "error_code": "LLM_CONFIGURATION_ERROR",
                    "status": "failed",
                },
            )
            raise
        except (AgentIntegrationError, LLMIntegrationError) as exc:
            duration_ms = _duration_ms(started_at)
            retryable_code = _retryable_llm_error_code(exc)
            error_code = llm_error_code(exc)
            final_attempt = attempt >= MAX_LLM_ATTEMPTS - 1
            if retryable_code is None or final_attempt:
                logger.warning(
                    "llm_call_failed",
                    extra={
                        **log_base,
                        "attempt": attempt + 1,
                        "max_attempts": MAX_LLM_ATTEMPTS,
                        "retry_count": attempt,
                        "duration_ms": duration_ms,
                        "error_code": error_code,
                        "status": "failed",
                    },
                )
                if isinstance(exc, LLMIntegrationError):
                    raise _as_service_error(exc) from exc
                raise
            logger.warning(
                "llm_call_retry",
                extra={
                    **log_base,
                    "attempt": attempt + 1,
                    "max_attempts": MAX_LLM_ATTEMPTS,
                    "retry_count": attempt + 1,
                    "duration_ms": duration_ms,
                    "error_code": error_code,
                    "status": "retrying",
                },
            )
            try:
                await _sleep_before_retry(abort_signal, attempt)
            except BaseException:
                if abort_signal is None or not abort_signal.aborted:
                    raise
                logger.info(
                    "llm_call_aborted",
                    extra={
                        **log_base,
                        "attempt": attempt + 1,
                        "max_attempts": MAX_LLM_ATTEMPTS,
                        "retry_count": attempt + 1,
                        "duration_ms": _duration_ms(started_at),
                        "error_code": "LLM_CALL_ABORTED",
                        "status": "aborted",
                    },
                )
                raise
        except asyncio.CancelledError:
            logger.info(
                "llm_call_aborted",
                extra={
                    **log_base,
                    "attempt": attempt + 1,
                    "max_attempts": MAX_LLM_ATTEMPTS,
                    "retry_count": attempt,
                    "duration_ms": _duration_ms(started_at),
                    "error_code": "LLM_CALL_ABORTED",
                    "status": "aborted",
                },
            )
            raise
        except ValueError as exc:
            logger.warning(
                "llm_call_failed",
                extra={
                    **log_base,
                    "attempt": attempt + 1,
                    "max_attempts": MAX_LLM_ATTEMPTS,
                    "retry_count": attempt,
                    "duration_ms": _duration_ms(started_at),
                    "error_code": "LLM_INVALID_RESPONSE",
                    "status": "failed",
                },
            )
            raise AgentIntegrationError(
                f"{label} returned invalid model response: {exc}",
                error_code=AgentErrorCode.INVALID_RESPONSE,
            ) from exc
        except Exception as exc:
            if abort_signal is not None and abort_signal.aborted:
                logger.info(
                    "llm_call_aborted",
                    extra={
                        **log_base,
                        "attempt": attempt + 1,
                        "max_attempts": MAX_LLM_ATTEMPTS,
                        "retry_count": attempt,
                        "duration_ms": _duration_ms(started_at),
                        "error_code": "LLM_CALL_ABORTED",
                        "status": "aborted",
                    },
                )
                raise
            logger.error(
                "llm_call_failed",
                extra={
                    **log_base,
                    "attempt": attempt + 1,
                    "max_attempts": MAX_LLM_ATTEMPTS,
                    "retry_count": attempt,
                    "duration_ms": _duration_ms(started_at),
                    "error_code": type(exc).__name__,
                    "status": "failed",
                },
                exc_info=True,
            )
            raise
    raise AgentIntegrationError(f"{label} LLM exhausted retries without a response")


async def generate_tool_loop_response(
    context: AgentContext,
    *,
    label: str,
    messages: list[ChatMessage],
    tools: list[ToolDefinition],
    llm_runtime: LlmRuntimeConfig | None = None,
    llm_client: LLMClientPort | None = None,
) -> ChatResponse:
    runtime = _resolve_runtime(context, llm_runtime)
    if runtime is None:
        raise AgentConfigurationError(f"{label} requires configured llm runtime")

    if llm_client is None:
        raise AgentConfigurationError(f"{label} llm client is not configured")
    protocol = runtime.protocol
    base_url = runtime.base_url
    api_key = runtime.api_key
    model = runtime.model
    temperature = runtime.temperature
    top_p = runtime.randomness
    chat = getattr(llm_client, "chat", None)
    if not callable(chat):
        raise AgentConfigurationError(f"{label} llm client does not support chat")
    client = llm_client
    abort_signal = _abort_signal(context)
    throw_if_aborted(abort_signal)
    estimated_input_tokens, effective_output_tokens = _effective_output_cap(
        runtime,
        messages=messages,
        tools=tools,
    )

    # Buffer assistant text until the turn finishes. Models often stream prose
    # before tool_calls in the same completion; publishing that prose as the
    # final response causes a brief flash that then disappears when tools run.
    text_buffer: list[str] = []
    input_payload = {"messages": messages, "tools": tools}
    log_base = _llm_log_base(
        context,
        label=label,
        runtime=runtime,
        llm_mode="chat",
        input_bytes=_payload_bytes(input_payload),
        max_output_tokens=effective_output_tokens,
    )
    log_base.update(
        {
            "message_count": len(messages),
            "tool_definition_count": len(tools),
            "estimated_input_tokens": estimated_input_tokens,
            "requested_max_output_tokens": runtime.max_output_tokens,
        }
    )
    result: object = None
    successful_attempt = 0
    successful_duration_ms = 0

    for attempt in range(MAX_LLM_ATTEMPTS):
        throw_if_aborted(abort_signal)
        text_buffer.clear()
        started_at = perf_counter()
        try:
            result = await _stream_or_chat(
                client,
                protocol=protocol,
                base_url=base_url,
                api_key=api_key,
                model=model,
                messages=messages,
                temperature=float(temperature),
                tools=tools,
                top_p=float(top_p),
                max_output_tokens=effective_output_tokens,
                thinking_effort=runtime.thinking_effort,
                provider_config=runtime.provider_config,
                on_text_delta=_buffering_text_delta_handler(text_buffer),
                on_reasoning_delta=_reasoning_delta_handler(context, label),
                abort_signal=abort_signal,
            )
            successful_duration_ms = _duration_ms(started_at)
            throw_if_aborted(abort_signal)
            successful_attempt = attempt + 1
            break
        except AgentConfigurationError:
            logger.error(
                "llm_call_failed",
                extra={
                    **log_base,
                    "attempt": attempt + 1,
                    "max_attempts": MAX_LLM_ATTEMPTS,
                    "retry_count": attempt,
                    "duration_ms": _duration_ms(started_at),
                    "error_code": "LLM_CONFIGURATION_ERROR",
                    "status": "failed",
                },
            )
            raise
        except (AgentIntegrationError, LLMIntegrationError) as exc:
            duration_ms = _duration_ms(started_at)
            retryable_code = _retryable_llm_error_code(exc)
            error_code = llm_error_code(exc)
            final_attempt = attempt >= MAX_LLM_ATTEMPTS - 1
            if retryable_code is None or final_attempt:
                logger.warning(
                    "llm_call_failed",
                    extra={
                        **log_base,
                        "attempt": attempt + 1,
                        "max_attempts": MAX_LLM_ATTEMPTS,
                        "retry_count": attempt,
                        "duration_ms": duration_ms,
                        "error_code": error_code,
                        "status": "failed",
                    },
                )
                if isinstance(exc, LLMIntegrationError):
                    raise _as_service_error(exc) from exc
                raise
            logger.warning(
                "llm_call_retry",
                extra={
                    **log_base,
                    "attempt": attempt + 1,
                    "max_attempts": MAX_LLM_ATTEMPTS,
                    "retry_count": attempt + 1,
                    "duration_ms": duration_ms,
                    "error_code": error_code,
                    "status": "retrying",
                },
            )
            try:
                await _sleep_before_retry(abort_signal, attempt)
            except BaseException:
                if abort_signal is None or not abort_signal.aborted:
                    raise
                logger.info(
                    "llm_call_aborted",
                    extra={
                        **log_base,
                        "attempt": attempt + 1,
                        "max_attempts": MAX_LLM_ATTEMPTS,
                        "retry_count": attempt + 1,
                        "duration_ms": _duration_ms(started_at),
                        "output_bytes": _payload_bytes(result),
                        "error_code": "LLM_CALL_ABORTED",
                        "status": "aborted",
                    },
                )
                raise
        except asyncio.CancelledError:
            logger.info(
                "llm_call_aborted",
                extra={
                    **log_base,
                    "attempt": attempt + 1,
                    "max_attempts": MAX_LLM_ATTEMPTS,
                    "retry_count": attempt,
                    "duration_ms": _duration_ms(started_at),
                    "output_bytes": _payload_bytes(result),
                    "error_code": "LLM_CALL_ABORTED",
                    "status": "aborted",
                },
            )
            raise
        except ValueError as exc:
            logger.warning(
                "llm_call_failed",
                extra={
                    **log_base,
                    "attempt": attempt + 1,
                    "max_attempts": MAX_LLM_ATTEMPTS,
                    "retry_count": attempt,
                    "duration_ms": _duration_ms(started_at),
                    "error_code": "LLM_INVALID_RESPONSE",
                    "status": "failed",
                },
            )
            raise AgentIntegrationError(
                f"{label} returned invalid chat response: {exc}",
                error_code=AgentErrorCode.INVALID_RESPONSE,
            ) from exc
        except Exception as exc:
            if abort_signal is not None and abort_signal.aborted:
                logger.info(
                    "llm_call_aborted",
                    extra={
                        **log_base,
                        "attempt": attempt + 1,
                        "max_attempts": MAX_LLM_ATTEMPTS,
                        "retry_count": attempt,
                        "duration_ms": _duration_ms(started_at),
                        "output_bytes": _payload_bytes(result),
                        "error_code": "LLM_CALL_ABORTED",
                        "status": "aborted",
                    },
                )
                raise
            logger.error(
                "llm_call_failed",
                extra={
                    **log_base,
                    "attempt": attempt + 1,
                    "max_attempts": MAX_LLM_ATTEMPTS,
                    "retry_count": attempt,
                    "duration_ms": _duration_ms(started_at),
                    "error_code": type(exc).__name__,
                    "status": "failed",
                },
                exc_info=True,
            )
            raise

    try:
        response = normalize_chat_response(result)
    except ValueError as exc:
        logger.warning(
            "llm_call_failed",
            extra={
                **log_base,
                "attempt": successful_attempt,
                "max_attempts": MAX_LLM_ATTEMPTS,
                "retry_count": max(0, successful_attempt - 1),
                "duration_ms": successful_duration_ms,
                "output_bytes": _payload_bytes(result),
                "error_code": "LLM_INVALID_RESPONSE",
                "status": "failed",
            },
        )
        raise AgentIntegrationError(
            f"{label} returned invalid chat response: {exc}",
            error_code=AgentErrorCode.INVALID_RESPONSE,
        ) from exc
    tool_calls = response["tool_calls"]
    content = response["content"]
    logger.info(
        "llm_call_completed",
        extra={
            **log_base,
            "attempt": successful_attempt,
            "max_attempts": MAX_LLM_ATTEMPTS,
            "retry_count": max(0, successful_attempt - 1),
            "duration_ms": successful_duration_ms,
            "output_bytes": _payload_bytes(result),
            "tool_call_count": len(tool_calls),
            "status": "completed",
        },
    )

    # Only the final turn (no tool_calls) is the final response. Publish now.
    if not tool_calls and response.get("stop_reason") not in {
        StopReason.LENGTH,
        StopReason.CONTEXT_OVERFLOW,
        StopReason.ERROR,
        StopReason.ABORTED,
    }:
        report_text = "".join(text_buffer)
        if not report_text and isinstance(content, str):
            report_text = content
        if report_text.strip():
            await _publish_buffered_report_text(context, label, report_text)

    return response
