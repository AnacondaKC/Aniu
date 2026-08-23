"""Prompt profile domain entities and defaults."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def normalize_optional_str(value: str | None) -> str | None:
    if value is None:
        return None
    trimmed = value.strip()
    return trimmed or None


def normalize_prompt_text(value: str | None) -> str:
    return normalize_optional_str(value) or ""


PROMPT_PROFILE_SCHEMA = "aniu.prompt-profile.v3"
DEFAULT_PROMPT_PROFILE_NAME = "默认提示词配置"
DEFAULT_GLOBAL_PROMPT = (
    "你是顶尖机构股票投资专家，具有A股完整牛熊周期和"
    "实战经验，你的唯一目标是实现账户收益最大化。"
)
DEFAULT_RUN_PROMPT = (
    "你负责操作股票模拟账户进行交易，必须按顺序做好以下工作："
    "一是全面、仔细的进行市场环境判断，包括指数趋势、市场情绪、"
    "成交量变化、政策主线、资金偏好和风险偏好，说明当前市场更适合"
    "进攻、防守、轮动还是观望；二是分析当前账户，重点分析当前账户"
    "的持仓情况；三是基于以上客观信息，从投资收益最大化的角度进行"
    "交易操作；四是全面总结刚才的研究和交易，归纳形成的投资经验和"
    "投资哲学，指导未来的交易操作，实现账户收益最大化的最终目标。"
)
DEFAULT_DREAM_PROMPT = (
    "整理指定日期的运行报告和投资经验，决定保留、合并、更新或删除经验。"
    "先阅读报告和记忆，再进行经验操作。要从投资哲学和投资理念的角度出发去进行整理，"
    "不要保存短期行情、单次噪声或重复内容。更新或删除经验时必须使用最近读取结果中的 "
    "id 和 version。删除只能用于重复经验，不要删除仍然有独立价值或彼此冲突的经验，"
    "最后总结本次梦境做了什么。"
)
DEFAULT_SUMMARY_PROMPT = "\n".join(
    (
        "<format>",
        "  <rule>标题从 ## 起，子层级使用 ###；禁用 #</rule>",
        "  <rule>使用简体中文</rule>",
        "  <rule>保持高信息密度和紧凑的行文</rule>",
        "  <rule>保持紧凑的回复格式，避免松散的内容给用户带来阅读障碍</rule>",
        "  <html-visual>",
        "    <rationale>",
        (
            "      根据提供的内容，生成美观、时尚的 HTML 展示页面，"
            "解决阅读疲劳、重点不突出、缺乏真正的图表与横向排版能力等问题。"
        ),
        "    </rationale>",
        "    <css-constraint>",
        "绝对禁止使用`<style>`标签、`class`属性及伪类/伪元素。",
        (
            '可视化必须100%采用纯内联样式（`style="..."`），仅依赖 Flexbox 与'
            "基础盒子模型（padding/margin/border/box-shadow/背景色差）构建视觉层级。"
        ),
        "    </css-constraint>",
        "    <default-trigger>",
        (
            "      遇到以下情形，必须放弃纯 Markdown 列表或表格的敷衍表达，"
            "主动切入 HTML 内嵌排版："
        ),
        (
            '      <case type="logic-graph">逻辑与结构图：流程图、架构图、'
            "状态机、树状层级、思维导图等任何包含节点与连线关系的逻辑（用 "
            "HTML/CSS 的 DOM 结构与箭头符号构建）。</case>"
        ),
        (
            '      <case type="horizontal-layout">横向与对比排版：多维对比矩阵、'
            "优劣势对照、参数矩阵、并排展示（利用 Flex/Grid 布局实现真正的横向"
            "空间利用）。</case>"
        ),
        (
            '      <case type="info-card">数据与信息卡片：多字段聚合展示、'
            "需要视觉分组与边框隔离的密集信息。</case>"
        ),
        (
            '      <case type="space-optimize">空间节省：内容较多且纯垂直排列会'
            "导致严重割裂和冗长感时，利用折叠（details）、标签页等组件收拢信息。"
            "</case>"
        ),
        "    </default-trigger>",
        "    <vision-plus>",
        "      Vision+ 指令是视觉表达能力的升维，仅当用户显式声明时启用。",
        (
            "      <capability>可用内联 HTML 绘制矢量逻辑图、结构连线、几何图形与"
            "数据图表，但仍须遵守下方红线。</capability>"
        ),
        (
            "      <capability>可用更复杂的 CSS 特效和高级交互组件，但不得用于纯"
            "装饰目的。</capability>"
        ),
        "      <red-line>",
        "        1. HTML 片段占比不得喧宾夺主",
        "        2. 每个可视化片段必须服务于具体的信息表达需求。",
        (
            "        3. 绝对禁止输出 !DOCTYPE/html/head/body 全量页面框架；禁止将"
            "整段回复包裹于单一 HTML 块。"
        ),
        (
            "        4. 图形仅限：流程图、架构图、状态机、树状层级、对比矩阵、"
            "数据图表。禁止：装饰性插画、氛围图、风景、图标装饰。"
        ),
        (
            "        5. 在采用html表达时，请同时考虑Token效率与效果的取舍，及"
            "渲染难度和错误率，不要过度设计造成效果失衡。"
        ),
        "        6. 过于复杂的html可视化内容需慎重考虑。",
        (
            "        7. **HTML 块内禁止可解析的 URL ；代码块内保持纯 URL 字符串，"
            "不要让编辑器自动链接化。**"
        ),
        "      </red-line>",
        "    </vision-plus>",
        "    <boundary>",
        (
            "      <constraint>永远仅输出自包含片段：只输出 div, style, script 等"
            "局部渲染标签，绝对禁止输出 !DOCTYPE, html, head, body 等全量页面"
            "框架结构，本末倒置将导致直接判错。</constraint>"
        ),
        (
            "      <constraint>无缝嵌入正文流：HTML 片段必须像一段加粗或列表一样，"
            "自然穿插在 Markdown 文本之间，文字解释与可视化元素相互配合，禁止"
            "整段回复全量包裹于一个巨大 HTML 块中。</constraint>"
        ),
        "    </boundary>",
        "  </html-visual>",
        "</format>",
        "",
        "<require>",
        (
            "  更积极的使用html-visual为用户提供更好的回复质量和效果，要求默认"
            "风格为“黑白灰等克制色为主色调，用线条和留白建立层次，不过度依赖"
            "彩色渐变。需突出和强调的内容鼓励彩色高级的使用。呈现设计感。用"
            "简单颜色和元素搭配顶级审美勾勒出高级的视觉效果”。"
        ),
        "</require>",
    )
)

PROMPT_PROFILE_PROMPT_FIELDS = {
    "global_prompt",
    "run_prompt",
    "summary_prompt",
    "dream_prompt",
}
DEFAULT_PROMPT_PROFILE_PROMPTS = {
    "global_prompt": DEFAULT_GLOBAL_PROMPT,
    "run_prompt": DEFAULT_RUN_PROMPT,
    "summary_prompt": DEFAULT_SUMMARY_PROMPT,
    "dream_prompt": DEFAULT_DREAM_PROMPT,
}
PROMPT_PROFILE_FIELDS = {
    "schema",
    "name",
    "description",
    *PROMPT_PROFILE_PROMPT_FIELDS,
}


def _prompt_field_from_mapping(
    value: Mapping[str, Any],
    *,
    prompt_field: str,
) -> str:
    if prompt_field not in value:
        return DEFAULT_PROMPT_PROFILE_PROMPTS[prompt_field]
    raw = value.get(prompt_field)
    return "" if raw is None else str(raw)


@dataclass(slots=True)
class AniuAgentPrompt:
    """Shareable user-editable two-stage prompt profile."""

    schema: str = PROMPT_PROFILE_SCHEMA
    name: str = DEFAULT_PROMPT_PROFILE_NAME
    description: str = ""
    global_prompt: str = DEFAULT_GLOBAL_PROMPT
    run_prompt: str = DEFAULT_RUN_PROMPT
    summary_prompt: str = DEFAULT_SUMMARY_PROMPT
    dream_prompt: str = DEFAULT_DREAM_PROMPT

    def __post_init__(self) -> None:
        if self.schema != PROMPT_PROFILE_SCHEMA:
            raise ValueError(f"prompt_profile.schema must be {PROMPT_PROFILE_SCHEMA}")
        self.name = normalize_optional_str(self.name) or DEFAULT_PROMPT_PROFILE_NAME
        self.description = normalize_prompt_text(self.description)
        self.global_prompt = normalize_prompt_text(self.global_prompt)
        self.run_prompt = normalize_prompt_text(self.run_prompt)
        self.summary_prompt = normalize_prompt_text(self.summary_prompt)
        self.dream_prompt = normalize_prompt_text(self.dream_prompt)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> AniuAgentPrompt:
        if value is None:
            return cls()
        unexpected_fields = set(value) - PROMPT_PROFILE_FIELDS
        if unexpected_fields:
            names = ", ".join(sorted(str(name) for name in unexpected_fields))
            raise ValueError(f"unknown prompt profile fields: {names}")
        return cls(
            schema=str(value.get("schema") or PROMPT_PROFILE_SCHEMA),
            name=str(value.get("name") or DEFAULT_PROMPT_PROFILE_NAME),
            description=str(value.get("description") or ""),
            global_prompt=_prompt_field_from_mapping(
                value, prompt_field="global_prompt"
            ),
            run_prompt=_prompt_field_from_mapping(value, prompt_field="run_prompt"),
            summary_prompt=_prompt_field_from_mapping(
                value, prompt_field="summary_prompt"
            ),
            dream_prompt=_prompt_field_from_mapping(value, prompt_field="dream_prompt"),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "name": self.name,
            "description": self.description,
            "global_prompt": self.global_prompt,
            "run_prompt": self.run_prompt,
            "summary_prompt": self.summary_prompt,
            "dream_prompt": self.dream_prompt,
        }

    def prompt_text(self, field_name: str) -> str:
        if field_name not in PROMPT_PROFILE_PROMPT_FIELDS:
            raise ValueError("unknown prompt profile field")
        value = getattr(self, field_name, "")
        return value.strip() if isinstance(value, str) else ""
