"""Compose Aniubot stage resources into a generic AgentHarness."""

from __future__ import annotations

import asyncio
import time
from typing import cast

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.agent.contracts import MessageAppended
from backend.agent.errors import AgentConfigurationError, AgentIntegrationError
from backend.agent.harness import AgentHarness
from backend.agent.json import json_safe, serialize_context
from backend.agent.kernel.context_budget import ContextBudgetExceededError
from backend.agent.kernel.runtime_config import LlmRuntimeConfig
from backend.business.runs import RunEventType, StrategySnapshot
from backend.business.runs.agent_runner import (
    AgentRunnerPort,
    AgentRuntimeBundle,
    AgentStageResult,
)
from backend.business.runs.execution import RunExecutionContext
from backend.business.runs.pipeline_stages import (
    is_write_stage,
    pipeline_stage_for_state_name,
)
from backend.business.shared import (
    IntegrationErrorCode,
    ServiceConfigurationError,
    ServiceIntegrationError,
)
from backend.business.shared.stock_api_source import stock_api_tool_context
from backend.business.stock_api_logs.catalog import stock_api_tool_descriptor
from backend.business.stock_api_logs.models import (
    StockApiToolCall,
    StockApiToolCallLogger,
)
from backend.infra.integrations.agent_runtime import AgentRuntimeFactory
from backend.infra.integrations.tool_policy import (
    SideEffectLevel,
    coerce_side_effect_level,
)
from backend.infra.repositories.tool_invocation_repo import ToolInvocationRepository
from backend.llm import AbortSignal, LLMClientPort
from backend.stock_api.public import (
    InvalidStockRequest,
    PublicStockDataError,
    UnsupportedStockRequest,
)


def _tool_enabled(tool: object, label: str) -> bool:
    enabled = getattr(tool, "enabled_stages", ())
    if not isinstance(enabled, (list, tuple, set, frozenset)):
        return False
    stage = pipeline_stage_for_state_name(label)
    if stage is None:
        return False
    return any(
        label == str(item)
        or stage.state_name == str(item)
        or label.startswith(f"{item}:")
        for item in enabled
    )


class _StageToolRegistry:
    def __init__(
        self,
        source: object,
        label: str,
        *,
        run_id: int,
        invocation_session_factory: async_sessionmaker[AsyncSession] | None,
        stock_api_tool_call_logger: StockApiToolCallLogger | None = None,
    ) -> None:
        self._source = source
        self._label = label
        self._run_id = run_id
        self._invocation_session_factory = invocation_session_factory
        self._stock_api_tool_call_logger = stock_api_tool_call_logger
        self._stock_api_calls_by_tool_call_id: dict[
            str, tuple[dict[str, object], ...]
        ] = {}
        list_tools = getattr(source, "list_tools", None)
        tools = list_tools() if callable(list_tools) else []
        self._tools = {
            str(getattr(tool, "name", "")): tool
            for tool in tools
            if _tool_enabled(tool, label)
            and (
                is_write_stage(label)
                or coerce_side_effect_level(getattr(tool, "side_effect_level", None))
                is not SideEffectLevel.WRITE
            )
        }

    def get(self, name: str) -> object:
        try:
            return self._tools[name]
        except KeyError as exc:
            raise KeyError(f"tool not available: {name}") from exc

    def list_tools(self) -> list[object]:
        return [self._tools[name] for name in sorted(self._tools)]

    async def call(self, name: str, **kwargs: object) -> object:
        self.get(name)
        call = getattr(self._source, "call")
        return await call(name, **kwargs)

    async def call_with_abort(
        self,
        name: str,
        *,
        abort_signal: AbortSignal | None,
        **kwargs: object,
    ) -> object:
        self.get(name)
        call = getattr(self._source, "call_with_abort", None)
        if callable(call):
            return await call(name, abort_signal=abort_signal, **kwargs)
        return await self.call(name, **kwargs)

    def stock_api_call_details(
        self, tool_call_id: str
    ) -> tuple[dict[str, object], ...]:
        return self._stock_api_calls_by_tool_call_id.get(tool_call_id, ())

    async def _record_stock_api_tool_call(
        self,
        *,
        name: str,
        parameters: dict[str, object],
        status: str,
        duration_ms: int,
        response_characters: int | None,
        error_category: str | None,
        error_message: str | None,
    ) -> None:
        descriptor = stock_api_tool_descriptor(name)
        logger = self._stock_api_tool_call_logger
        if descriptor is None or logger is None:
            return
        try:
            await logger(
                StockApiToolCall(
                    tool_source=descriptor.tool_source,
                    tool_id=descriptor.tool_id,
                    parameters=parameters,
                    status=status,
                    duration_ms=duration_ms,
                    response_characters=response_characters,
                    error_category=error_category,
                    error_message=error_message,
                )
            )
        except Exception:
            return

    async def _call_with_stock_api_context(
        self,
        name: str,
        *,
        tool_call_id: str,
        abort_signal: AbortSignal | None,
        **kwargs: object,
    ) -> object:
        tool = self.get(name)
        safe_parameters = json_safe(kwargs)
        parameters = safe_parameters if isinstance(safe_parameters, dict) else {}
        log_parameters = getattr(tool, "stock_api_log_parameters", None)
        if callable(log_parameters):
            try:
                enriched = log_parameters(parameters)
                if isinstance(enriched, dict):
                    parameters = enriched
            except Exception:
                pass
        started_at = time.perf_counter()
        status = "success"
        response_characters: int | None = None
        error_category: str | None = None
        error_message: str | None = None
        with stock_api_tool_context(
            run_id=self._run_id,
            stage_name=self._label,
            tool_call_id=tool_call_id,
            tool_name=name,
        ) as calls:
            try:
                run_for_call = getattr(tool, "run_for_call", None)
                if callable(run_for_call):
                    result = await run_for_call(
                        run_id=self._run_id,
                        tool_call_id=tool_call_id,
                        abort_signal=abort_signal,
                        **kwargs,
                    )
                else:
                    result = await self.call_with_abort(
                        name,
                        abort_signal=abort_signal,
                        **kwargs,
                    )
                response_characters = len(serialize_context(json_safe(result)))
                if (
                    isinstance(result, dict)
                    and str(result.get("status") or "") == "error"
                ):
                    status = "failed"
                    error_category = "unknown"
                    error_message = str(result.get("error") or "tool returned error")
                return result
            except asyncio.CancelledError:
                status = "failed"
                error_category = "cancelled"
                error_message = "tool call cancelled"
                raise
            except PublicStockDataError as exc:
                status = "failed"
                error_category = exc.error_category or (
                    "business_failure"
                    if isinstance(exc, (InvalidStockRequest, UnsupportedStockRequest))
                    else "unknown"
                )
                error_message = str(exc) or type(exc).__name__
                raise
            except Exception as exc:
                status = "failed"
                error_category = "unknown"
                error_message = str(exc) or type(exc).__name__
                raise
            finally:
                self._stock_api_calls_by_tool_call_id[tool_call_id] = tuple(
                    dict(call) for call in calls
                )
                await self._record_stock_api_tool_call(
                    name=name,
                    parameters=parameters,
                    status=status,
                    duration_ms=round((time.perf_counter() - started_at) * 1000),
                    response_characters=response_characters,
                    error_category=error_category,
                    error_message=error_message,
                )

    async def call_idempotently(
        self,
        name: str,
        *,
        tool_call_id: str,
        abort_signal: AbortSignal | None,
        **kwargs: object,
    ) -> object:
        tool = self.get(name)
        is_write_call = getattr(tool, "is_write_call", None)
        writes = (
            bool(is_write_call(kwargs))
            if callable(is_write_call)
            else coerce_side_effect_level(getattr(tool, "side_effect_level", None))
            is SideEffectLevel.WRITE
        )
        factory = self._invocation_session_factory
        if not writes or factory is None:
            return await self._call_with_stock_api_context(
                name,
                tool_call_id=tool_call_id,
                abort_signal=abort_signal,
                **kwargs,
            )

        safe_arguments = json_safe(kwargs)
        arguments = safe_arguments if isinstance(safe_arguments, dict) else {}
        async with factory() as session:
            reservation = await ToolInvocationRepository(session).reserve(
                run_id=self._run_id,
                tool_call_id=tool_call_id,
                tool_name=name,
                arguments=arguments,
            )
            await session.commit()
        if reservation.is_completed:
            return reservation.completed_result

        try:
            result = await self._call_with_stock_api_context(
                name,
                tool_call_id=tool_call_id,
                abort_signal=abort_signal,
                **kwargs,
            )
        except Exception as exc:
            # The outcome is known to be an error, but the call ID must still
            # be finalized so a replay cannot repeat a possibly external write.
            try:
                async with factory() as session:
                    await ToolInvocationRepository(session).fail(
                        run_id=self._run_id,
                        tool_call_id=tool_call_id,
                        error=str(exc),
                    )
                    await session.commit()
            except Exception:
                pass
            raise
        safe_result = json_safe(result)
        async with factory() as session:
            await ToolInvocationRepository(session).complete(
                run_id=self._run_id,
                tool_call_id=tool_call_id,
                result=safe_result,
            )
            await session.commit()
        return result


class AgentRunnerAdapter(AgentRunnerPort):
    def __init__(
        self,
        *,
        harness: AgentHarness,
        label: str,
    ) -> None:
        self._harness = harness
        self._label = label

    async def prepare_prompt(
        self,
        message: str,
        *,
        preserve_prefix: str = "",
        abort_signal: AbortSignal | None = None,
    ) -> str:
        try:
            return await self._harness.prepare_prompt(
                message,
                preserve_prefix=preserve_prefix,
                abort_signal=abort_signal,
            )
        except AgentConfigurationError as exc:
            raise ServiceConfigurationError(
                str(exc), status_code=exc.status_code
            ) from exc
        except ContextBudgetExceededError as exc:
            raise ServiceIntegrationError(
                str(exc),
                error_code=IntegrationErrorCode.CONTEXT_OVERFLOW,
            ) from exc
        except AgentIntegrationError as exc:
            try:
                error_code = IntegrationErrorCode(exc.error_code.value)
            except ValueError:
                error_code = IntegrationErrorCode.UNKNOWN
            raise ServiceIntegrationError(
                str(exc),
                status_code=exc.status_code,
                error_code=error_code,
            ) from exc

    async def prompt(
        self,
        message: str,
        *,
        abort_signal: AbortSignal | None = None,
    ) -> AgentStageResult:
        try:
            result = await self._harness.prompt(message, abort_signal=abort_signal)
        except AgentConfigurationError as exc:
            raise ServiceConfigurationError(
                str(exc), status_code=exc.status_code
            ) from exc
        except ContextBudgetExceededError as exc:
            raise ServiceIntegrationError(
                str(exc),
                error_code=IntegrationErrorCode.CONTEXT_OVERFLOW,
            ) from exc
        except AgentIntegrationError as exc:
            try:
                error_code = IntegrationErrorCode(exc.error_code.value)
            except ValueError:
                error_code = IntegrationErrorCode.UNKNOWN
            raise ServiceIntegrationError(
                str(exc),
                status_code=exc.status_code,
                error_code=error_code,
            ) from exc
        activity = tuple(
            {"stage_name": self._label, **dict(item)} for item in result.tool_activity
        )
        transcript = tuple(
            dict(mutation.message)
            for mutation in result.session_mutations
            if isinstance(mutation, MessageAppended)
        )
        return AgentStageResult(
            content=result.content,
            tool_activity=activity,
            transcript=transcript,
        )


class AgentRunnerFactoryAdapter:
    def __init__(
        self,
        llm_client: LLMClientPort,
        runtime_factory: AgentRuntimeFactory,
        *,
        invocation_session_factory: async_sessionmaker[AsyncSession] | None = None,
        stock_api_tool_call_logger: StockApiToolCallLogger | None = None,
    ) -> None:
        self._llm_client = llm_client
        self._runtime_factory = runtime_factory
        self._invocation_session_factory = invocation_session_factory
        self._stock_api_tool_call_logger = stock_api_tool_call_logger

    async def prepare(self, snapshot: object) -> AgentRuntimeBundle:
        strategy = cast(StrategySnapshot, snapshot)
        registry = await self._runtime_factory.build_tool_registry()
        runtimes = await self._runtime_factory.build_stage_runtimes(strategy)
        return AgentRuntimeBundle(
            tool_registry=registry,
            stage_runtimes=dict(runtimes),
        )

    def create(
        self,
        context: object,
        *,
        label: str,
        runtime: object,
    ) -> AgentRunnerPort:
        workflow = cast(RunExecutionContext, context)
        stage_registry = _StageToolRegistry(
            workflow.tool_registry,
            label,
            run_id=workflow.run.run_id,
            invocation_session_factory=self._invocation_session_factory,
            stock_api_tool_call_logger=self._stock_api_tool_call_logger,
        )
        system_prompt = ""

        async def event_sink(event_name: str, payload: dict[str, object]) -> None:
            sink = workflow.tool_loop_event_sink
            if not callable(sink):
                return
            try:
                event_type = RunEventType(event_name)
            except ValueError:
                return
            await sink(label, event_type, {"stage_name": label, **payload})

        async def stream_sink(delta: str, channel: str) -> None:
            sink = workflow.llm_stream_delta_sink
            if callable(sink):
                await sink(label, delta, channel=channel)

        def authorize(tool: object, arguments: dict[str, object]) -> str | None:
            is_write_call = getattr(tool, "is_write_call", None)
            writes = (
                bool(is_write_call(arguments))
                if callable(is_write_call)
                else coerce_side_effect_level(getattr(tool, "side_effect_level", None))
                is SideEffectLevel.WRITE
            )
            if writes and bool(getattr(tool, "requires_market_open", False)):
                market_open = workflow.market_session_is_open
                if not callable(market_open) or not market_open():
                    return "非交易时段，交易和撤单写操作已被阻止；仍可继续分析。"
            return None

        harness = AgentHarness(
            runtime=cast(LlmRuntimeConfig, runtime),
            llm_client=self._llm_client,
            system_prompt=system_prompt,
            tool_registry=stage_registry,
            event_sink=event_sink,
            stream_sink=stream_sink,
            tool_authorizer=authorize,
            label=label,
        )
        return AgentRunnerAdapter(
            harness=harness,
            label=label,
        )


__all__ = ["AgentRunnerAdapter", "AgentRunnerFactoryAdapter"]
