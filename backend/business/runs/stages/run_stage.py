"""Run stage: research, decide, trade, and produce one Markdown report."""

from __future__ import annotations

from backend.business.runs.agent_runner import AgentRunnerPort
from backend.business.runs.execution import RunExecutionContext, RunReport
from backend.business.runs.stages.stage_helpers import (
    emit_stage_prompt_prepared,
    require_llm_runtime,
)
from backend.business.shared.serialization import serialize_context

_MEMORY_TOOL_PROTOCOL = "\n".join(
    (
        "<memory-tools>",
        (
            "形成买入、卖出、调仓、撤单或观望判断前，应主动调用 memory_read "
            "查询相关经验及可能反驳当前判断的经验。"
        ),
        "经验只是历史，必须结合当前市场、账户和工具事实全面判断。",
        "最终只输出 Markdown 报告正文。",
        (
            "如果有值得复用的经验和教训时调用 memory_write，"
            "保存简洁的投资经验教训和形成原因，"
            "不得保存运行总结、短暂行情或泛泛原则。"
        ),
        "</memory-tools>",
    )
)


class RunStage:
    async def execute(
        self,
        context: RunExecutionContext,
        agent_runner: AgentRunnerPort,
    ) -> RunReport:
        require_llm_runtime(context, stage_name="Run")
        stage_settings = context.snapshot.settings_for_stage("Run")
        market_open = (
            bool(context.market_session_is_open())
            if callable(context.market_session_is_open)
            else False
        )
        runtime_payload: dict[str, object] = {"market_session_open": market_open}
        agent_prompt = "\n\n".join((stage_settings.prompt, _MEMORY_TOOL_PROTOCOL))
        user_prompt = "\n\n".join(
            (
                agent_prompt,
                "runtime_context:\n" + serialize_context(runtime_payload),
            )
        )
        await emit_stage_prompt_prepared(
            context,
            stage_name="Run",
            phase="run_input",
            title="任务提示词",
            summary="已发送任务规则、记忆工具协议与当前交易时段状态",
            display_prompt=agent_prompt,
            payload=runtime_payload,
            user_message=user_prompt,
        )
        result = await agent_runner.prompt(
            user_prompt,
            abort_signal=context.abort_signal,
        )
        content = result.content.strip()
        if not content:
            raise ValueError("run report must not be empty")
        return RunReport(
            content=content,
            tool_activity=result.tool_activity,
            transcript=result.transcript,
        )


__all__ = ["RunStage"]
