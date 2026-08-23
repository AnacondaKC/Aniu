"""Generic agent loop with model-driven tool use."""

from __future__ import annotations

import asyncio
from typing import cast

from backend.agent.actors.tool_executor import (
    ToolCallRegistry,
    emit_agent_event,
    event_type_for_result,
    execute_tool_call_batch,
    normalize_tool_call,
    trace_tool_arguments,
)
from backend.agent.actors.tool_loop import ToolLoopResult
from backend.agent.contracts import (
    AgentResult,
    CompactionCheckpoint,
    MessageAppended,
    SessionMutation,
)
from backend.agent.errors import AgentErrorCode, AgentIntegrationError
from backend.agent.events import AgentEventType
from backend.agent.kernel.abort import throw_if_aborted
from backend.agent.kernel.context import AgentContext
from backend.agent.kernel.context_budget import (
    ContextBudgetConfig,
    ContextBudgetExceededError,
)
from backend.agent.kernel.context_compaction import compact_context_messages
from backend.agent.kernel.llm_runtime import (
    generate_text_output,
    generate_tool_loop_response,
)
from backend.agent.kernel.runtime_config import (
    DEFAULT_MAX_PARALLEL_TOOL_CALLS,
    LlmRuntimeConfig,
)
from backend.agent.tools.definitions import list_tool_definitions
from backend.llm import (
    ChatMessage,
    ChatResponse,
    LLMClientPort,
    LLMToolCall,
    StopReason,
    ToolDefinition,
    assistant_input_from_message,
    estimate_provider_message_tokens,
    estimate_provider_request_tokens,
)


async def emit_tool_loop_event(
    context: AgentContext,
    _label: str,
    event_type: AgentEventType,
    payload: dict[str, object],
) -> None:
    await emit_agent_event(context, event_type, payload)


def list_available_turn_tools(registry: object | None) -> list[ToolDefinition]:
    """Return exactly the definitions exposed by an initial tool-loop request."""

    return list_tool_definitions(registry)


def build_initial_turn_messages(
    *,
    history: tuple[ChatMessage, ...],
    system_prompt: str,
    user_prompt: str,
    available_tools: list[ToolDefinition],
) -> tuple[list[ChatMessage], str]:
    """Build the same initial transcript used for both budgeting and execution."""

    messages: list[ChatMessage] = list(history)
    system_message = system_prompt.strip()
    if not system_message and available_tools:
        tool_names = [str(tool["name"]) for tool in available_tools]
        system_message = f"Tool protocol: {', '.join(tool_names)}."
    if system_message and not messages:
        messages.append({"role": "system", "content": system_message})
    messages.append({"role": "user", "content": user_prompt})
    return messages, system_message


def _estimate_provider_request_context(
    messages: list[ChatMessage],
    tools: list[ToolDefinition],
    runtime: LlmRuntimeConfig | None,
) -> int:
    if runtime is None:
        return estimate_provider_request_tokens(messages, tools)
    return estimate_provider_request_tokens(
        messages,
        tools,
        protocol=runtime.protocol,
        model=runtime.model,
        provider_config=runtime.provider_config,
    )


def _estimate_provider_message_context(
    messages: list[ChatMessage],
    runtime: LlmRuntimeConfig | None,
) -> int:
    if runtime is None:
        return estimate_provider_message_tokens(messages)
    return estimate_provider_message_tokens(
        messages,
        protocol=runtime.protocol,
        model=runtime.model,
        provider_config=runtime.provider_config,
    )


def _require_complete_turn(response: ChatResponse, label: str) -> None:
    stop_reason = response.get("stop_reason")
    if stop_reason is StopReason.CONTEXT_OVERFLOW:
        raise AgentIntegrationError(
            f"{label} provider context window was exceeded",
            error_code=AgentErrorCode.CONTEXT_OVERFLOW,
        )
    if stop_reason is StopReason.LENGTH:
        raise AgentIntegrationError(
            f"{label} model output reached the token limit before completion"
        )
    if stop_reason in {StopReason.ERROR, StopReason.ABORTED}:
        raise AgentIntegrationError(f"{label} model ended with {stop_reason.value}")


def _append_assistant_response(
    messages: list[ChatMessage],
    response: ChatResponse,
) -> ChatMessage:
    assistant_message = response.get("assistant_message")
    if assistant_message is not None:
        appended = assistant_input_from_message(assistant_message)
    else:
        content = response.get("content")
        appended = {
            "role": "assistant",
            "content": content if content is not None and content.strip() else None,
            "tool_calls": response.get("tool_calls", []),
            "reasoning": response.get("reasoning"),
        }
    messages.append(appended)
    return appended


def _model_tool_message(
    result: ToolLoopResult,
) -> tuple[dict[str, object], str]:
    """Build the trace payload and pre-compaction Agent-context content."""

    content = result.as_model_text()
    payload = result.as_payload()
    payload["model_content_characters"] = len(content)
    return payload, content


def _is_context_overflow_error(exc: AgentIntegrationError) -> bool:
    return exc.error_code is AgentErrorCode.CONTEXT_OVERFLOW


async def _compact_messages_for_next_turn(
    context: AgentContext,
    *,
    label: str,
    messages: list[ChatMessage],
    config: ContextBudgetConfig,
    llm_tools: list[ToolDefinition],
    llm_runtime: LlmRuntimeConfig | None,
    llm_client: LLMClientPort | None,
    force: bool = False,
) -> tuple[list[ChatMessage], CompactionCheckpoint | None]:
    async def generate_summary(
        system_prompt: str, user_prompt: str, max_output_tokens: int
    ) -> str:
        return await generate_text_output(
            context,
            label=f"{label}:compaction",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            llm_runtime=llm_runtime,
            llm_client=llm_client,
            publish_stream=False,
            max_output_tokens=max_output_tokens,
        )

    def estimate_context(selected: list[ChatMessage]) -> int:
        return _estimate_provider_request_context(selected, llm_tools, llm_runtime)

    def estimate_history(selected: list[ChatMessage]) -> int:
        return _estimate_provider_message_context(selected, llm_runtime)

    def estimate_summary_input(system_prompt: str, user_prompt: str) -> int:
        return _estimate_provider_request_context(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            [],
            llm_runtime,
        )

    result = await compact_context_messages(
        messages,
        config=config,
        summary_generator=generate_summary,
        force=force,
        context_token_estimator=estimate_context,
        history_token_estimator=estimate_history,
        summary_input_token_estimator=estimate_summary_input,
    )
    checkpoint: CompactionCheckpoint | None = None
    if result.was_compacted:
        assert result.summary is not None
        checkpoint = CompactionCheckpoint(
            summary=result.summary,
            retained_messages=result.retained_messages,
            original_tokens=result.original_tokens,
            compacted_tokens=result.compacted_tokens,
            summarized_message_count=result.summarized_message_count,
            cause="provider_overflow" if force else "proactive",
        )
        await emit_tool_loop_event(
            context,
            label,
            AgentEventType.CONTEXT_COMPACTED,
            {
                "label": context.label,
                "original_tokens": result.original_tokens,
                "compacted_tokens": result.compacted_tokens,
                "summarized_message_count": result.summarized_message_count,
            },
        )
    return result.messages, checkpoint


async def _generate_turn_with_context_recovery(
    context: AgentContext,
    *,
    label: str,
    messages: list[ChatMessage],
    tools: list[ToolDefinition],
    config: ContextBudgetConfig,
    llm_runtime: LlmRuntimeConfig | None,
    llm_client: LLMClientPort | None,
) -> tuple[list[ChatMessage], ChatResponse, CompactionCheckpoint | None]:
    """Run one turn and retry exactly once after forced history compaction."""

    async def generate(current_messages: list[ChatMessage]) -> ChatResponse:
        response = await generate_tool_loop_response(
            context,
            label=f"{label}:tool_loop",
            messages=current_messages,
            tools=tools,
            llm_runtime=llm_runtime,
            llm_client=llm_client,
        )
        _require_complete_turn(response, label)
        return response

    try:
        return messages, await generate(messages), None
    except AgentIntegrationError as exc:
        if not _is_context_overflow_error(exc):
            raise

    compacted_messages, checkpoint = await _compact_messages_for_next_turn(
        context,
        label=label,
        messages=messages,
        config=config,
        llm_tools=tools,
        llm_runtime=llm_runtime,
        llm_client=llm_client,
        force=True,
    )
    return compacted_messages, await generate(compacted_messages), checkpoint


class AgentLoop:
    """Run one generic model/tool conversation until final content."""

    async def run(
        self,
        context: AgentContext,
        user_prompt: str,
        history: tuple[ChatMessage, ...] = (),
    ) -> AgentResult:
        label = context.label
        llm_runtime = context.runtime
        llm_client = context.llm_client
        abort_signal = context.abort_signal
        throw_if_aborted(abort_signal)
        registry = context.tool_registry
        available_tools = list_available_turn_tools(registry)
        llm_tools = available_tools
        budget_config = self._budget_config(context, llm_runtime)
        activity: list[dict[str, object]] = []

        messages, system_message = build_initial_turn_messages(
            history=history,
            system_prompt=context.system_prompt,
            user_prompt=user_prompt,
            available_tools=available_tools,
        )
        session_mutations: list[SessionMutation] = [
            MessageAppended(message) for message in messages[len(history) :]
        ]
        await emit_tool_loop_event(
            context,
            label,
            AgentEventType.TOOL_LOOP_STARTED,
            {
                "label": context.label,
                "available_tools": [tool["name"] for tool in available_tools],
                "tool_definitions": [dict(tool) for tool in available_tools],
                "system_message": system_message,
                "user_message": user_prompt,
            },
        )
        iteration = 0
        sequence = 0
        while True:
            throw_if_aborted(abort_signal)
            iteration += 1
            try:
                messages, checkpoint = await _compact_messages_for_next_turn(
                    context,
                    label=label,
                    messages=messages,
                    config=budget_config,
                    llm_tools=llm_tools,
                    llm_runtime=llm_runtime,
                    llm_client=llm_client,
                )
                if checkpoint is not None:
                    session_mutations.append(checkpoint)
                (
                    messages,
                    response,
                    recovery_checkpoint,
                ) = await _generate_turn_with_context_recovery(
                    context,
                    label=label,
                    messages=messages,
                    tools=llm_tools,
                    config=budget_config,
                    llm_runtime=llm_runtime,
                    llm_client=llm_client,
                )
                if recovery_checkpoint is not None:
                    session_mutations.append(recovery_checkpoint)
            except ContextBudgetExceededError as exc:
                result = ToolLoopResult(
                    tool_call_id="context-budget",
                    tool_name="context_budget",
                    arguments={},
                    status="error",
                    error=str(exc),
                    iteration=iteration,
                    sequence=sequence,
                )
                payload = result.as_payload()
                activity.append(payload)
                await emit_tool_loop_event(
                    context,
                    label,
                    AgentEventType.TOOL_LOOP_FAILED,
                    payload,
                )
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                result = ToolLoopResult(
                    tool_call_id="tool-loop",
                    tool_name="tool_loop",
                    arguments={},
                    status="error",
                    error=str(exc),
                    iteration=iteration,
                    sequence=sequence,
                )
                payload = result.as_payload()
                activity.append(payload)
                await emit_tool_loop_event(
                    context,
                    label,
                    AgentEventType.TOOL_LOOP_FAILED,
                    payload,
                )
                raise

            tool_calls = response["tool_calls"]
            content = response["content"]
            if not tool_calls:
                final_content = content.strip() if content is not None else ""
                await emit_tool_loop_event(
                    context,
                    label,
                    AgentEventType.TOOL_LOOP_STOPPED,
                    {
                        "label": context.label,
                        "iteration": iteration,
                        "tool_calls_count": sequence,
                        "reason": "final_content" if final_content else "empty_content",
                    },
                )
                assistant_message = _append_assistant_response(messages, response)
                session_mutations.append(MessageAppended(assistant_message))
                return AgentResult(
                    content=final_content,
                    messages=tuple(messages),
                    session_mutations=tuple(session_mutations),
                    tool_activity=tuple(activity),
                    iterations=iteration,
                )

            normalized_calls = [
                normalize_tool_call(raw_call, index)
                for index, raw_call in enumerate(tool_calls)
            ]
            assistant_message = _append_assistant_response(messages, response)
            session_mutations.append(MessageAppended(assistant_message))
            calls_to_execute: list[tuple[object, LLMToolCall, int]] = []
            for raw_call, normalized_call in zip(
                tool_calls,
                normalized_calls,
                strict=True,
            ):
                throw_if_aborted(abort_signal)
                sequence += 1
                await emit_tool_loop_event(
                    context,
                    label,
                    AgentEventType.TOOL_CALL_REQUESTED,
                    {
                        "label": context.label,
                        "tool_call_id": normalized_call["id"],
                        "tool_name": normalized_call["name"],
                        "arguments": trace_tool_arguments(
                            registry,
                            normalized_call["name"],
                            normalized_call["arguments"],
                        ),
                    },
                )
                calls_to_execute.append((raw_call, normalized_call, sequence))

            async def emit_completed_result(result: ToolLoopResult) -> None:
                record, _ = _model_tool_message(result)
                await emit_tool_loop_event(
                    context,
                    label,
                    event_type_for_result(result),
                    record,
                )

            results = await execute_tool_call_batch(
                context,
                cast(ToolCallRegistry | None, registry),
                calls_to_execute,
                iteration=iteration,
                max_concurrency=self._max_parallel_tool_calls(context, llm_runtime),
                on_result=emit_completed_result,
            )
            for normalized_call, result in zip(
                normalized_calls,
                results,
                strict=True,
            ):
                record, tool_text = _model_tool_message(result)
                activity.append(record)
                tool_message: ChatMessage = {
                    "role": "tool",
                    "tool_call_id": normalized_call["id"],
                    "name": normalized_call["name"],
                    "content": tool_text,
                    "is_error": result.is_error,
                }
                messages.append(tool_message)
                session_mutations.append(MessageAppended(tool_message))

    def _max_parallel_tool_calls(
        self,
        context: AgentContext,
        override: LlmRuntimeConfig | None = None,
    ) -> int:
        runtime = override if override is not None else context.runtime
        if runtime is None:
            return DEFAULT_MAX_PARALLEL_TOOL_CALLS
        return max(1, runtime.max_parallel_tool_calls)

    def _budget_config(
        self,
        context: AgentContext,
        override: LlmRuntimeConfig | None = None,
    ) -> ContextBudgetConfig:
        runtime = override if override is not None else context.runtime
        return ContextBudgetConfig.from_runtime(runtime)
