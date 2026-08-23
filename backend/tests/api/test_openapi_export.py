"""OpenAPI export contract tests."""

from __future__ import annotations

from scripts.export_openapi import export_openapi


def test_openapi_export_has_json_response_schemas() -> None:
    schema = export_openapi()
    assert schema["info"]["title"] == "Aniu"
    assert "AniuSessionCookie" in schema["components"]["securitySchemes"]

    missing: list[str] = []
    for path, methods in schema["paths"].items():
        for method, operation in methods.items():
            if method not in {"get", "post", "put", "delete", "patch"}:
                continue
            responses = operation.get("responses", {})
            for status_code, response in responses.items():
                if not str(status_code).startswith("2"):
                    continue
                if status_code in {"204"}:
                    continue
                content = response.get("content") or {}
                if "text/event-stream" in content:
                    continue
                json_body = content.get("application/json")
                if json_body is None:
                    # Some routes may return empty bodies.
                    continue
                if "schema" not in json_body:
                    missing.append(f"{method.upper()} {path} {status_code}")
    assert missing == []


def test_openapi_lists_main_business_paths() -> None:
    schema = export_openapi()
    paths = set(schema["paths"])
    assert "/api/aniu/runs" in paths
    assert all("/artifacts/" not in path for path in paths)
    assert "/api/aniu/settings" in paths
    assert not any(path.startswith("/api/aniu/skills") for path in paths)
    schemas = schema["components"]["schemas"]
    assert "SkillResponse" not in schemas
    assert "SkillOriginResponse" not in schemas
    assert "UpdateSkillRequest" not in schemas
    assert "UpdateSkillTrustRequest" not in schemas
    for schema_name in (
        "StageSettingsRequest",
        "StageSettingsResponse",
        "StartRunRequest",
    ):
        assert "allowed_skills" not in schemas[schema_name]["properties"]
        assert "skills_enabled" not in schemas[schema_name]["properties"]
    run_trace = schemas["RunTraceResponse"]
    trace_stage = schemas["TraceStageResponse"]
    trace_step = schemas["TraceStepResponse"]
    trace_tool_call = schemas["TraceToolCallResponse"]
    run_summary = schemas["RunSummaryResponse"]
    run_detail = schemas["RunDetailResponse"]
    assert run_trace["properties"]["schema_version"]["const"] == 3
    assert "updated_at" not in run_trace["properties"]
    assert {"title", "description", "summary", "round"}.isdisjoint(
        trace_stage["properties"]
    )
    assert "trade_count" not in trace_stage["properties"]
    assert "trade_stage_status" not in run_summary["properties"]
    assert "summary_render_mode" in run_summary["properties"]
    assert "failure_reason" in run_detail["properties"]
    assert trace_step["properties"]["type"]["enum"] == [
        "thinking",
        "tool",
        "result",
        "status",
    ]
    assert "data" not in trace_step["properties"]
    assert "artifact" not in trace_step["properties"]
    assert "tool_call" in trace_step["properties"]
    assert "model_content_characters" in trace_tool_call["properties"]
    assert trace_step["properties"]["status"]["enum"] == [
        "pending",
        "running",
        "completed",
        "failed",
        "blocked",
    ]
    assert "ErrorResponse" in schemas
    sse_response = schema["paths"]["/api/aniu/sse/run/{run_id}"]["get"]["responses"][
        "200"
    ]
    assert "text/event-stream" in sse_response["content"]
    assert schema["paths"]["/api/aniu/runs"]["get"]["responses"]["401"]["content"][
        "application/json"
    ]["schema"]["$ref"].endswith("/ErrorResponse")
    assert "/api/aniu/auth/session" in paths
    dream_detail = schema["paths"]["/api/aniu/memory-dreams/{task_id}"]["get"]
    assert "404" in dream_detail["responses"]
    assert dream_detail["responses"]["404"]["content"]["application/json"][
        "schema"
    ]["$ref"].endswith("/ErrorResponse")

    expected_error_statuses = {
        "/api/aniu/runs": {"401", "403", "404", "409", "422", "503"},
        "/api/aniu/account/dashboard": {"401", "403", "422", "429", "502", "503"},
        "/api/aniu/schedules": {"401", "403", "404", "409", "422"},
        "/api/aniu/settings": {"401", "403", "404", "409", "422", "502", "503"},
        "/api/aniu/sse/run/{run_id}": {"401", "403", "404", "422"},
    }
    for path, statuses in expected_error_statuses.items():
        actual = set(schema["paths"][path]["get"]["responses"]) - {"200"}
        assert actual == statuses
