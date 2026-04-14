from __future__ import annotations

import json
from typing import Any

import httpx

from app.services.mx_skill_service import mx_skill_service
from app.services.mx_service import MXClient

_LLM_TEMPERATURE = 0.2
_MAX_TOOL_ITERATIONS = 100


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
    """Only pass normalized + summary to the model; keep raw out of context."""
    return {
        "ok": tool_result.get("ok"),
        "tool_name": tool_result.get("tool_name"),
        "summary": tool_result.get("summary"),
        "result": tool_result.get("normalized"),
    }


class LLMService:
    def __init__(self) -> None:
        self._http_client: httpx.Client | None = None

    def _get_http_client(self, timeout_seconds: int) -> httpx.Client:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.Client(timeout=float(timeout_seconds))
        else:
            self._http_client.timeout = httpx.Timeout(float(timeout_seconds))
        return self._http_client

    def close(self) -> None:
        if self._http_client is not None and not self._http_client.is_closed:
            self._http_client.close()
            self._http_client = None

    def chat(
        self,
        *,
        model: str,
        base_url: str,
        api_key: str,
        system_prompt: str | None,
        messages: list[dict[str, str]],
        timeout_seconds: int = 60,
    ) -> str:
        payload_messages: list[dict[str, str]] = []
        if system_prompt:
            payload_messages.append({"role": "system", "content": system_prompt})
        payload_messages.extend(messages)

        response_payload = self._call_llm(
            base_url=base_url,
            api_key=api_key,
            payload={
                "model": model,
                "temperature": _LLM_TEMPERATURE,
                "messages": payload_messages,
            },
            timeout_seconds=timeout_seconds,
        )

        choices = response_payload.get("choices") or []
        if not choices:
            raise RuntimeError("大模型未返回 choices。")

        message = choices[0].get("message") or {}
        content = _to_text_content(message.get("content"))
        return content or "模型本轮未返回可展示内容。"

    def build_initial_request_payload(self, app_settings: Any) -> dict[str, Any]:
        run_type = str(getattr(app_settings, "run_type", "analysis") or "analysis")
        return {
            "model": app_settings.llm_model,
            "temperature": _LLM_TEMPERATURE,
            "messages": [
                {"role": "system", "content": app_settings.system_prompt},
                {"role": "user", "content": getattr(app_settings, "task_prompt", "")},
            ],
            "tools": mx_skill_service.build_tools(run_type=run_type),
            "tool_choice": "auto",
        }

    def run_agent(
        self,
        app_settings: Any,
        client: MXClient,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
        request_payload = self.build_initial_request_payload(app_settings)
        if not app_settings.llm_base_url or not app_settings.llm_api_key:
            raise RuntimeError("未配置大模型接口，无法执行 AI 调度。")

        messages = [dict(message) for message in request_payload["messages"]]
        response_history: list[dict[str, Any]] = []
        tool_history: list[dict[str, Any]] = []
        final_message = ""
        run_type = str(getattr(app_settings, "run_type", "analysis") or "analysis")

        for _ in range(_MAX_TOOL_ITERATIONS):
            iteration_payload = {
                "model": app_settings.llm_model,
                "temperature": _LLM_TEMPERATURE,
                "messages": messages,
                "tools": mx_skill_service.build_tools(run_type=run_type),
                "tool_choice": "auto",
            }
            response_payload = self._call_llm(
                base_url=app_settings.llm_base_url,
                api_key=app_settings.llm_api_key,
                payload=iteration_payload,
                timeout_seconds=getattr(app_settings, "timeout_seconds", 60),
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

            tool_calls = message.get("tool_calls") or []
            if not tool_calls:
                final_message = _to_text_content(message.get("content"))
                if not final_message:
                    final_message = "模型本轮未返回可展示内容。"
                return (
                    {
                        "final_answer": final_message,
                        "tool_calls": tool_history,
                    },
                    request_payload,
                    {
                        "responses": response_history,
                        "final_message": message,
                    },
                    {"messages": messages},
                )

            for tool_call in tool_calls:
                if not isinstance(tool_call, dict):
                    continue
                function_payload = tool_call.get("function") or {}
                tool_name = str(function_payload.get("name") or "").strip()
                arguments_text = function_payload.get("arguments") or "{}"
                try:
                    arguments = json.loads(arguments_text)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"工具参数不是合法 JSON: {exc}") from exc

                tool_result = self._execute_tool_call(
                    client=client,
                    app_settings=app_settings,
                    tool_name=tool_name,
                    arguments=arguments,
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

    def _execute_tool_call(
        self,
        *,
        client: MXClient,
        app_settings: Any,
        tool_name: str,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        return mx_skill_service.execute_tool(
            client=client,
            app_settings=app_settings,
            tool_name=tool_name,
            arguments=arguments,
        )

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
        http_client = self._get_http_client(timeout_seconds)
        try:
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
