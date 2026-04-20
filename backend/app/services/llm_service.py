from __future__ import annotations

import json
import threading
from typing import Any, Callable, Iterable

import httpx

from app.services.mx_service import MXClient
from app.skills import skill_registry

_LLM_TEMPERATURE = 0.2
_MAX_TOOL_ITERATIONS = 100
_FINAL_STREAM_CHUNK_SIZE = 96
_CHAT_CONFIRMATION_APPEND_PROMPT = (
    "聊天专用安全规则：当操作涉及交易执行、下单、撤单、自选股增删、写入、删除、覆盖、"
    "批量修改或其他会改变数据、文件、配置、状态的破坏性操作时，你必须先明确说明拟执行操作、"
    "影响范围和潜在风险，并在得到用户明确确认后才能调用工具或执行操作；若未获得明确确认，"
    "只能提供方案、预览或建议，不得直接执行。"
)


class LLMStreamCancelled(RuntimeError):
    """Raised when a streaming chat/run should stop because the client disconnected."""


def _raise_if_cancelled(cancel_event: threading.Event | None) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise LLMStreamCancelled("客户端连接已断开。")


def _to_text_content(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "text" and isinstance(item.get("text"), str):
                parts.append(item["text"])
        return "\n".join(parts).strip()
    return ""


def _safe_json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _slim_tool_result(tool_result: dict[str, Any]) -> dict[str, Any]:
    """Pass raw tool payloads to the model while keeping minimal metadata."""
    return {
        "ok": tool_result.get("ok"),
        "tool_name": tool_result.get("tool_name"),
        "summary": tool_result.get("summary"),
        "result": tool_result.get("result"),
    }


def _iter_text_chunks(content: str, chunk_size: int = _FINAL_STREAM_CHUNK_SIZE):
    text = str(content or "")
    if not text:
        return

    for block in text.splitlines(keepends=True):
        if len(block) <= chunk_size:
            yield block
            continue

        start = 0
        while start < len(block):
            yield block[start : start + chunk_size]
            start += chunk_size


def _to_stream_text_content(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "text" and isinstance(item.get("text"), str):
                parts.append(item["text"])
        return "".join(parts)
    return ""


def _merge_stream_tool_call(
    tool_calls: dict[int, dict[str, Any]],
    delta_payload: dict[str, Any],
) -> None:
    index = int(delta_payload.get("index") or 0)
    entry = tool_calls.setdefault(
        index,
        {
            "id": "",
            "type": "function",
            "function": {"name": "", "arguments": ""},
        },
    )

    call_id = delta_payload.get("id")
    if isinstance(call_id, str) and call_id:
        entry["id"] = call_id

    call_type = delta_payload.get("type")
    if isinstance(call_type, str) and call_type:
        entry["type"] = call_type

    function_payload = delta_payload.get("function")
    if not isinstance(function_payload, dict):
        return

    function_entry = entry.setdefault("function", {"name": "", "arguments": ""})
    name = function_payload.get("name")
    if isinstance(name, str) and name:
        function_entry["name"] += name

    arguments = function_payload.get("arguments")
    if isinstance(arguments, str) and arguments:
        function_entry["arguments"] += arguments


class LLMService:
    def _create_http_client(self, timeout_seconds: int) -> httpx.Client:
        return httpx.Client(timeout=float(timeout_seconds))

    def close(self) -> None:
        return None

    def chat(
        self,
        *,
        model: str,
        base_url: str,
        api_key: str,
        system_prompt: str | None,
        messages: list[dict[str, str]],
        timeout_seconds: int = 60,
        tool_context: dict[str, Any] | None = None,
        emit: Any = None,
        cancel_event: threading.Event | None = None,
    ) -> str:
        payload_messages: list[dict[str, Any]] = []
        effective_system_prompt = self._augment_system_prompt(
            system_prompt,
            run_type="chat",
        )
        if effective_system_prompt:
            payload_messages.append(
                {"role": "system", "content": effective_system_prompt}
            )
        payload_messages.extend(messages)
        chat_tool_context = {**(tool_context or {}), "run_type": "chat"}

        def _chat_tool_executor(tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
            return skill_registry.execute_tool(
                tool_name=tool_name,
                arguments=arguments,
                context=chat_tool_context,
            )

        result = self._agent_loop(
            model=model,
            base_url=base_url,
            api_key=api_key,
            initial_messages=payload_messages,
            run_type="chat",
            timeout_seconds=timeout_seconds,
            tool_executor=_chat_tool_executor,
            emit=emit,
            cancel_event=cancel_event,
        )
        return result["final_answer"] or "模型本轮未返回可展示内容。"

    def build_initial_request_payload(self, app_settings: Any) -> dict[str, Any]:
        run_type = str(getattr(app_settings, "run_type", "analysis") or "analysis")
        system_prompt = self._augment_system_prompt(
            app_settings.system_prompt,
            run_type=run_type,
        )
        return {
            "model": app_settings.llm_model,
            "temperature": _LLM_TEMPERATURE,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": getattr(app_settings, "task_prompt", "")},
            ],
            "tools": skill_registry.build_tools(run_type=run_type),
            "tool_choice": "auto",
        }

    def build_request_payload_from_messages(
        self,
        *,
        app_settings: Any,
        messages: list[dict[str, Any]],
    ) -> dict[str, Any]:
        run_type = str(getattr(app_settings, "run_type", "analysis") or "analysis")
        system_prompt = self._augment_system_prompt(
            app_settings.system_prompt,
            run_type=run_type,
        )
        payload_messages: list[dict[str, Any]] = []
        if system_prompt:
            payload_messages.append({"role": "system", "content": system_prompt})
        payload_messages.extend(dict(message) for message in messages)
        return {
            "model": app_settings.llm_model,
            "temperature": _LLM_TEMPERATURE,
            "messages": payload_messages,
            "tools": skill_registry.build_tools(run_type=run_type),
            "tool_choice": "auto",
        }

    @staticmethod
    def _augment_system_prompt(
        base_prompt: str | None,
        *,
        run_type: str | None = None,
    ) -> str:
        supplement = skill_registry.build_prompt_supplement(run_type=run_type)
        prompt_parts = [
            str(base_prompt or "").strip(),
            str(supplement or "").strip(),
        ]
        if str(run_type or "").strip() == "chat":
            prompt_parts.append(_CHAT_CONFIRMATION_APPEND_PROMPT)
        return "\n\n".join(part for part in prompt_parts if part)

    def run_agent(
        self,
        app_settings: Any,
        client: MXClient,
        emit: Any = None,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
        return self.run_agent_with_messages(
            app_settings=app_settings,
            client=client,
            messages=[
                {
                    "role": "user",
                    "content": getattr(app_settings, "task_prompt", ""),
                }
            ],
            emit=emit,
        )

    def run_agent_with_messages(
        self,
        *,
        app_settings: Any,
        client: MXClient,
        messages: list[dict[str, Any]],
        emit: Any = None,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
        request_payload = self.build_request_payload_from_messages(
            app_settings=app_settings,
            messages=messages,
        )
        if not app_settings.llm_base_url or not app_settings.llm_api_key:
            raise RuntimeError("未配置大模型接口，无法执行 AI 调度。")

        run_type = str(getattr(app_settings, "run_type", "analysis") or "analysis")

        def _run_tool_executor(tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
            return skill_registry.execute_tool(
                tool_name=tool_name,
                arguments=arguments,
                context={
                    "client": client,
                    "app_settings": app_settings,
                    "run_type": run_type,
                },
            )

        result = self._agent_loop(
            model=app_settings.llm_model,
            base_url=app_settings.llm_base_url,
            api_key=app_settings.llm_api_key,
            initial_messages=[dict(m) for m in request_payload["messages"]],
            run_type=run_type,
            timeout_seconds=getattr(app_settings, "timeout_seconds", 60),
            tool_executor=_run_tool_executor,
            emit=emit,
        )

        return (
            {
                "final_answer": result["final_answer"],
                "tool_calls": result["tool_history"],
            },
            request_payload,
            {
                "responses": result["responses"],
                "final_message": result["final_message"],
            },
            {"messages": result["messages"]},
        )

    def _agent_loop(
        self,
        *,
        model: str,
        base_url: str,
        api_key: str,
        initial_messages: list[dict[str, Any]],
        run_type: str,
        timeout_seconds: int,
        tool_executor: Callable[[str, dict[str, Any]], dict[str, Any]],
        emit: Any = None,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        _emit = emit if callable(emit) else (lambda *_a, **_kw: None)
        messages: list[dict[str, Any]] = [dict(m) for m in initial_messages]
        response_history: list[dict[str, Any]] = []
        tool_history: list[dict[str, Any]] = []

        for iteration in range(_MAX_TOOL_ITERATIONS):
            _raise_if_cancelled(cancel_event)
            iteration_payload = {
                "model": model,
                "temperature": _LLM_TEMPERATURE,
                "messages": messages,
                "tools": skill_registry.build_tools(run_type=run_type),
                "tool_choice": "auto",
            }
            _emit("llm_request", iteration=iteration + 1, model=model)
            response_payload = self._call_llm_stream(
                base_url=base_url,
                api_key=api_key,
                payload=iteration_payload,
                timeout_seconds=timeout_seconds,
                emit=_emit,
                cancel_event=cancel_event,
            )
            response_history.append(response_payload)

            choices = response_payload.get("choices") or []
            if not choices:
                raise RuntimeError("大模型未返回 choices。")

            message = choices[0].get("message") or {}
            assistant_entry: dict[str, Any] = {
                "role": "assistant",
                "content": message.get("content") or "",
            }
            if message.get("tool_calls"):
                assistant_entry["tool_calls"] = message["tool_calls"]
            messages.append(assistant_entry)

            assistant_text = _to_text_content(message.get("content"))
            tool_calls = message.get("tool_calls") or []
            if assistant_text and tool_calls:
                _emit("llm_message", iteration=iteration + 1, content=assistant_text)

            if not tool_calls:
                final_message = assistant_text or "模型本轮未返回可展示内容。"
                stream_meta = response_payload.get("stream_meta")
                final_streamed = (
                    isinstance(stream_meta, dict)
                    and bool(stream_meta.get("final_streamed"))
                )
                if not final_streamed:
                    self._emit_final_answer_stream(final_message, emit=_emit)
                return {
                    "final_answer": final_message,
                    "tool_history": tool_history,
                    "responses": response_history,
                    "final_message": message,
                    "messages": messages,
                }

            for tool_call in tool_calls:
                _raise_if_cancelled(cancel_event)
                if not isinstance(tool_call, dict):
                    continue
                function_payload = tool_call.get("function") or {}
                tool_name = str(function_payload.get("name") or "").strip()
                arguments_text = function_payload.get("arguments") or "{}"
                try:
                    arguments = json.loads(arguments_text)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"工具参数不是合法 JSON: {exc}") from exc

                _emit(
                    "tool_call",
                    phase="llm",
                    tool_name=tool_name,
                    tool_call_id=tool_call.get("id"),
                    arguments=arguments,
                    status="running",
                )
                tool_result = tool_executor(tool_name, arguments)
                _emit(
                    "tool_call",
                    phase="llm",
                    tool_name=tool_name,
                    tool_call_id=tool_call.get("id"),
                    arguments=arguments,
                    status="done",
                    ok=bool(tool_result.get("ok")),
                    summary=tool_result.get("summary"),
                )
                tool_history.append(
                    {
                        "id": tool_call.get("id"),
                        "name": tool_name,
                        "arguments": arguments,
                        "result": tool_result,
                    }
                )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.get("id"),
                        "content": _safe_json_dumps(_slim_tool_result(tool_result)),
                    }
                )

        raise RuntimeError("大模型工具调用轮次超限，已中止。")

    def _emit_final_answer_stream(self, content: str, *, emit: Callable[..., Any]) -> None:
        final_text = str(content or "").strip()
        emit("final_started", char_count=len(final_text))

        streamed = 0
        for chunk in _iter_text_chunks(final_text):
            streamed += len(chunk)
            emit(
                "final_delta",
                delta=chunk,
                streamed_chars=streamed,
            )

        emit("final_finished", content=final_text, char_count=len(final_text))

    def _call_llm_stream(
        self,
        *,
        base_url: str,
        api_key: str,
        payload: dict[str, Any],
        timeout_seconds: int,
        emit: Callable[..., Any] | None = None,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        stream_payload = dict(payload)
        stream_payload["stream"] = True

        last_error: RuntimeError | None = None
        for include_usage in (True, False):
            _raise_if_cancelled(cancel_event)
            attempt_payload = dict(stream_payload)
            if include_usage:
                attempt_payload["stream_options"] = {"include_usage": True}
            try:
                return self._consume_llm_stream(
                    base_url=base_url,
                    api_key=api_key,
                    payload=attempt_payload,
                    timeout_seconds=timeout_seconds,
                    emit=emit,
                    cancel_event=cancel_event,
                )
            except RuntimeError as exc:
                last_error = exc
                if not include_usage or "参数错误 (400)" not in str(exc):
                    raise

        if last_error is not None:
            raise last_error
        raise RuntimeError("大模型流式请求失败。")

    def _consume_llm_stream(
        self,
        *,
        base_url: str,
        api_key: str,
        payload: dict[str, Any],
        timeout_seconds: int,
        emit: Callable[..., Any] | None = None,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        _emit = emit if callable(emit) else (lambda *_a, **_kw: None)
        _raise_if_cancelled(cancel_event)

        url = base_url.rstrip("/")
        if not url.endswith("/chat/completions"):
            url = f"{url}/chat/completions"

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        try:
            with self._create_http_client(timeout_seconds) as http_client:
                with http_client.stream(
                    "POST",
                    url,
                    headers=headers,
                    json=payload,
                ) as response:
                    response.raise_for_status()
                    return self._parse_llm_stream_response(
                        lines=response.iter_lines(),
                        emit=_emit,
                        cancel_event=cancel_event,
                    )
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status == 401:
                raise RuntimeError("大模型 API Key 无效或已过期 (401)。") from exc
            if status == 400:
                detail = ""
                try:
                    detail = exc.response.json().get("error", {}).get("message", "")
                except Exception:
                    pass
                raise RuntimeError(
                    f"大模型请求参数错误 (400): {detail or exc.response.text[:200]}"
                ) from exc
            if status == 429:
                raise RuntimeError(
                    "大模型接口请求频率超限 (429)，请稍后重试。"
                ) from exc
            raise RuntimeError(
                f"大模型接口返回错误 ({status}): {exc.response.text[:200]}"
            ) from exc
        except httpx.TimeoutException as exc:
            raise RuntimeError(
                f"大模型接口请求超时 ({timeout_seconds}s)，请检查网络或增加超时时间。"
            ) from exc

    def _parse_llm_stream_response(
        self,
        *,
        lines: Iterable[str],
        emit: Callable[..., Any],
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        data_lines: list[str] = []
        content_parts: list[str] = []
        tool_calls: dict[int, dict[str, Any]] = {}
        usage: dict[str, Any] | None = None
        finish_reason: str | None = None
        response_id: str | None = None
        response_model: str | None = None
        response_created: Any = None
        response_object: str | None = None
        chunk_count = 0
        stream_mode: str | None = None
        final_started = False

        def _flush_payload(raw_payload: str) -> None:
            nonlocal usage
            nonlocal finish_reason
            nonlocal response_id
            nonlocal response_model
            nonlocal response_created
            nonlocal response_object
            nonlocal chunk_count
            nonlocal stream_mode
            nonlocal final_started

            if not raw_payload:
                return
            if raw_payload == "[DONE]":
                return

            chunk = json.loads(raw_payload)
            chunk_count += 1

            if isinstance(chunk.get("usage"), dict):
                usage = chunk["usage"]

            if response_id is None and isinstance(chunk.get("id"), str):
                response_id = chunk["id"]
            if response_model is None and isinstance(chunk.get("model"), str):
                response_model = chunk["model"]
            if response_object is None and isinstance(chunk.get("object"), str):
                response_object = chunk["object"]
            if response_created is None and chunk.get("created") is not None:
                response_created = chunk.get("created")

            choices = chunk.get("choices")
            if not isinstance(choices, list) or not choices:
                return

            choice = choices[0] if isinstance(choices[0], dict) else {}
            finish_value = choice.get("finish_reason")
            if isinstance(finish_value, str) and finish_value:
                finish_reason = finish_value

            delta = choice.get("delta")
            if not isinstance(delta, dict):
                return

            delta_tool_calls = delta.get("tool_calls")
            if isinstance(delta_tool_calls, list) and delta_tool_calls:
                if stream_mode is None:
                    stream_mode = "tool"
                for item in delta_tool_calls:
                    if isinstance(item, dict):
                        _merge_stream_tool_call(tool_calls, item)

            delta_text = _to_stream_text_content(delta.get("content"))
            if delta_text:
                content_parts.append(delta_text)
                if stream_mode is None:
                    stream_mode = "final"
                if stream_mode == "final":
                    if not final_started:
                        emit("final_started")
                        final_started = True
                    emit("final_delta", delta=delta_text)

        for raw_line in lines:
            _raise_if_cancelled(cancel_event)
            line = raw_line if isinstance(raw_line, str) else raw_line.decode("utf-8")
            line = line.rstrip("\r\n")
            if not line:
                payload = "\n".join(data_lines)
                data_lines.clear()
                _flush_payload(payload)
                if payload == "[DONE]":
                    break
                continue
            if line.startswith(":"):
                continue
            if line.startswith("data:"):
                data_lines.append(line[5:].lstrip())

        if data_lines:
            _flush_payload("\n".join(data_lines))

        final_text = "".join(content_parts)
        if stream_mode != "tool":
            if not final_started:
                emit("final_started", char_count=len(final_text))
                final_started = True
            emit("final_finished", content=final_text, char_count=len(final_text))

        message: dict[str, Any] = {
            "role": "assistant",
            "content": final_text,
        }
        ordered_tool_calls = [tool_calls[idx] for idx in sorted(tool_calls)]
        if ordered_tool_calls:
            message["tool_calls"] = ordered_tool_calls

        response_payload: dict[str, Any] = {
            "id": response_id,
            "object": response_object or "chat.completion",
            "created": response_created,
            "model": response_model,
            "choices": [
                {
                    "index": 0,
                    "message": message,
                    "finish_reason": finish_reason,
                }
            ],
            "stream_meta": {
                "chunk_count": chunk_count,
                "final_streamed": stream_mode != "tool",
            },
        }
        if usage is not None:
            response_payload["usage"] = usage
        return response_payload

    def _call_llm(
        self,
        *,
        base_url: str,
        api_key: str,
        payload: dict[str, Any],
        timeout_seconds: int,
    ) -> dict[str, Any]:
        url = base_url.rstrip("/")
        if not url.endswith("/chat/completions"):
            url = f"{url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        try:
            with self._create_http_client(timeout_seconds) as http_client:
                response = http_client.post(url, headers=headers, json=payload)
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status == 401:
                raise RuntimeError("大模型 API Key 无效或已过期 (401)。") from exc
            if status == 400:
                detail = ""
                try:
                    detail = exc.response.json().get("error", {}).get("message", "")
                except Exception:
                    pass
                raise RuntimeError(
                    f"大模型请求参数错误 (400): {detail or exc.response.text[:200]}"
                ) from exc
            if status == 429:
                raise RuntimeError(
                    "大模型接口请求频率超限 (429)，请稍后重试。"
                ) from exc
            raise RuntimeError(
                f"大模型接口返回错误 ({status}): {exc.response.text[:200]}"
            ) from exc
        except httpx.TimeoutException as exc:
            raise RuntimeError(
                f"大模型接口请求超时 ({timeout_seconds}s)，请检查网络或增加超时时间。"
            ) from exc
        return response.json()


llm_service = LLMService()
