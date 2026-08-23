"""Stateful, business-neutral agent harness inspired by pi-agent-core."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from backend.agent.actors.agent_loop import (
    AgentLoop,
    build_initial_turn_messages,
    list_available_turn_tools,
)
from backend.agent.contracts import AgentResult
from backend.agent.events import AgentEventType
from backend.agent.kernel.abort import throw_if_aborted
from backend.agent.kernel.context import (
    AgentContext,
    AgentEventSink,
    AgentStreamSink,
    ToolAuthorization,
)
from backend.agent.kernel.context_budget import ContextBudgetConfig
from backend.agent.kernel.context_compaction import compact_user_prompt
from backend.agent.kernel.llm_runtime import generate_text_output
from backend.agent.kernel.runtime_config import LlmRuntimeConfig
from backend.agent.session import AgentSession
from backend.llm import (
    AbortSignal,
    ChatMessage,
    LLMClientPort,
    estimate_provider_request_tokens,
)


@dataclass(slots=True)
class _AbortController:
    _event: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def aborted(self) -> bool:
        return self._event.is_set()

    def abort(self) -> None:
        self._event.set()

    async def wait(self) -> None:
        await self._event.wait()

    def throw_if_aborted(self) -> None:
        if self.aborted:
            raise asyncio.CancelledError("agent operation aborted")


@dataclass(frozen=True, slots=True)
class _CombinedAbortSignal:
    internal: _AbortController
    external: AbortSignal | None

    @property
    def aborted(self) -> bool:
        return self.internal.aborted or bool(self.external and self.external.aborted)

    async def wait(self) -> None:
        if self.external is None:
            await self.internal.wait()
            return
        internal_wait = asyncio.create_task(self.internal.wait())
        external_wait = asyncio.create_task(self.external.wait())
        try:
            done, _ = await asyncio.wait(
                {internal_wait, external_wait},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                task.result()
        finally:
            for task in (internal_wait, external_wait):
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                internal_wait,
                external_wait,
                return_exceptions=True,
            )

    def throw_if_aborted(self) -> None:
        self.internal.throw_if_aborted()
        if self.external is not None:
            self.external.throw_if_aborted()


@dataclass(frozen=True, slots=True)
class TurnSnapshot:
    runtime: LlmRuntimeConfig
    system_prompt: str
    tool_registry: object | None
    label: str


class AgentHarness:
    """Own one transcript and execute generic model/tool turns."""

    def __init__(
        self,
        *,
        runtime: LlmRuntimeConfig,
        llm_client: LLMClientPort,
        system_prompt: str = "",
        tool_registry: object | None = None,
        event_sink: AgentEventSink | None = None,
        stream_sink: AgentStreamSink | None = None,
        tool_authorizer: ToolAuthorization | None = None,
        label: str = "agent",
        session: AgentSession | None = None,
        loop: AgentLoop | None = None,
    ) -> None:
        self._runtime = runtime
        self._llm_client = llm_client
        self._system_prompt = system_prompt
        self._tool_registry = tool_registry
        self._event_sink = event_sink
        self._stream_sink = stream_sink
        self._tool_authorizer = tool_authorizer
        self._label = label
        self._session = session or AgentSession()
        self._loop = loop or AgentLoop()
        self._active_abort: _AbortController | None = None

    @property
    def session(self) -> AgentSession:
        return self._session

    def create_turn_snapshot(self) -> TurnSnapshot:
        return TurnSnapshot(
            runtime=self._runtime,
            system_prompt=self._system_prompt,
            tool_registry=self._tool_registry,
            label=self._label,
        )

    async def prepare_prompt(
        self,
        message: str,
        *,
        preserve_prefix: str = "",
        abort_signal: AbortSignal | None = None,
    ) -> str:
        """Compact an oversized stage handoff before its first model request."""

        if self._active_abort is not None:
            raise RuntimeError("AgentHarness is already processing")
        snapshot = self.create_turn_snapshot()
        internal_abort = _AbortController()
        self._active_abort = internal_abort
        signal = _CombinedAbortSignal(internal_abort, abort_signal)
        context = AgentContext(
            runtime=snapshot.runtime,
            llm_client=self._llm_client,
            system_prompt=snapshot.system_prompt,
            tool_registry=snapshot.tool_registry,
            abort_signal=signal,
            event_sink=self._event_sink,
            stream_sink=self._stream_sink,
            tool_authorizer=self._tool_authorizer,
            label=snapshot.label,
        )
        try:
            throw_if_aborted(signal)
            tools = list_available_turn_tools(snapshot.tool_registry)

            def build_messages(user_prompt: str) -> list[ChatMessage]:
                messages, _ = build_initial_turn_messages(
                    history=self._session.messages,
                    system_prompt=snapshot.system_prompt,
                    user_prompt=user_prompt,
                    available_tools=tools,
                )
                return messages

            def estimate_context(messages: list[ChatMessage]) -> int:
                return estimate_provider_request_tokens(
                    messages,
                    tools,
                    protocol=snapshot.runtime.protocol,
                    model=snapshot.runtime.model,
                    provider_config=snapshot.runtime.provider_config,
                )

            def estimate_summary_input(system_prompt: str, user_prompt: str) -> int:
                return estimate_provider_request_tokens(
                    [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    [],
                    protocol=snapshot.runtime.protocol,
                    model=snapshot.runtime.model,
                    provider_config=snapshot.runtime.provider_config,
                )

            async def generate_summary(
                system_prompt: str,
                user_prompt: str,
                max_output_tokens: int,
            ) -> str:
                return await generate_text_output(
                    context,
                    label=f"{snapshot.label}:stage_input_compaction",
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    llm_runtime=snapshot.runtime,
                    llm_client=self._llm_client,
                    publish_stream=False,
                    max_output_tokens=max_output_tokens,
                )

            result = await compact_user_prompt(
                message,
                preserve_prefix=preserve_prefix,
                config=ContextBudgetConfig.from_runtime(snapshot.runtime),
                message_builder=build_messages,
                context_token_estimator=estimate_context,
                summary_generator=generate_summary,
                summary_input_token_estimator=estimate_summary_input,
            )
            if result.was_compacted:
                await self._emit(
                    AgentEventType.CONTEXT_COMPACTED,
                    {
                        "label": snapshot.label,
                        "scope": "stage_input",
                        "original_tokens": result.original_tokens,
                        "compacted_tokens": result.compacted_tokens,
                        "summarized_message_count": 1,
                    },
                )
            throw_if_aborted(signal)
            return result.user_prompt
        finally:
            self._active_abort = None

    async def prompt(
        self,
        message: str,
        *,
        abort_signal: AbortSignal | None = None,
    ) -> AgentResult:
        if self._active_abort is not None:
            raise RuntimeError("AgentHarness is already processing")
        snapshot = self.create_turn_snapshot()
        internal_abort = _AbortController()
        self._active_abort = internal_abort
        signal = _CombinedAbortSignal(internal_abort, abort_signal)
        context = AgentContext(
            runtime=snapshot.runtime,
            llm_client=self._llm_client,
            system_prompt=snapshot.system_prompt,
            tool_registry=snapshot.tool_registry,
            abort_signal=signal,
            event_sink=self._event_sink,
            stream_sink=self._stream_sink,
            tool_authorizer=self._tool_authorizer,
            label=snapshot.label,
        )
        try:
            await self._emit(AgentEventType.AGENT_STARTED, {"label": snapshot.label})
            await self._emit(AgentEventType.TURN_STARTED, {"label": snapshot.label})
            result = await self._loop.run(context, message, self._session.messages)
            if result.messages and not result.session_mutations:
                raise RuntimeError(
                    "agent loop returned messages without session mutations"
                )
            self._session.commit_turn(result.session_mutations)
            await self._emit(
                AgentEventType.TURN_COMPLETED,
                {"iterations": result.iterations},
            )
            await self._emit(AgentEventType.AGENT_COMPLETED, {})
            return result
        except BaseException:
            event = (
                AgentEventType.AGENT_ABORTED
                if signal.aborted
                else AgentEventType.AGENT_FAILED
            )
            await self._emit(event, {})
            raise
        finally:
            self._active_abort = None

    def abort(self) -> None:
        if self._active_abort is not None:
            self._active_abort.abort()

    async def _emit(
        self,
        event_type: AgentEventType,
        payload: dict[str, object],
    ) -> None:
        if self._event_sink is not None:
            await self._event_sink(event_type.value, payload)


__all__ = ["AgentHarness", "TurnSnapshot"]
