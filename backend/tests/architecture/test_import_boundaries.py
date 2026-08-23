"""Static architecture boundary checks for the modular monolith."""

from __future__ import annotations

import ast
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[2]

# Top-level package import rules (src may import dst).
ALLOWED: dict[str, set[str]] = {
    "llm": {"llm"},
    "agent": {"agent", "llm"},
    "stock_api": {"stock_api", "business"},
    "business": {"business", "llm"},
    "infra": {"infra", "agent", "business", "llm", "stock_api"},
    "api": {"api", "business"},
    "bootstrap": {
        "bootstrap",
        "api",
        "business",
        "infra",
        "llm",
        "stock_api",
    },
}


def _top_level_imports(path: Path) -> list[tuple[int, str]]:
    """Collect absolute package imports at any AST depth (including lazy imports)."""

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.append((node.lineno, alias.name))
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            found.append((node.lineno, node.module))
    return found


def _package_of(path: Path) -> str | None:
    rel = path.relative_to(BACKEND_ROOT)
    if not rel.parts:
        return None
    return rel.parts[0]


def test_no_forbidden_top_level_package_imports() -> None:
    violations: list[str] = []
    for path in BACKEND_ROOT.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        src = _package_of(path)
        if src is None or src not in ALLOWED:
            continue
        for lineno, module in _top_level_imports(path):
            if not module.startswith("backend."):
                continue
            parts = module.split(".")
            if len(parts) < 2:
                continue
            dst = parts[1]
            if dst not in ALLOWED:
                continue
            if dst not in ALLOWED[src]:
                rel = path.relative_to(BACKEND_ROOT.parent)
                violations.append(f"{rel}:{lineno} {src} -> {dst} ({module})")
    assert not violations, "Forbidden package imports:\n" + "\n".join(violations)


def test_no_module_import_cycles() -> None:
    modules: dict[str, Path] = {}
    for path in BACKEND_ROOT.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        rel = path.relative_to(BACKEND_ROOT.parent).with_suffix("")
        name = (
            ".".join(rel.parts[:-1]) if rel.name == "__init__" else ".".join(rel.parts)
        )
        modules[name] = path

    adjacency: dict[str, set[str]] = {name: set() for name in modules}
    for name, path in modules.items():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            imports: list[str] = []
            if isinstance(node, ast.Import):
                imports = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                base = node.module or ""
                if node.level:
                    parts = name.split(".")[:-1]
                    keep = parts[: len(parts) - node.level + 1]
                    base = ".".join(keep + ([base] if base else []))
                imports = [base]
            for imported in imports:
                candidate = imported
                while candidate:
                    if candidate in modules:
                        adjacency[name].add(candidate)
                        break
                    candidate = candidate.rpartition(".")[0]

    index = 0
    stack: list[str] = []
    on_stack: set[str] = set()
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    components: list[list[str]] = []

    def strongconnect(vertex: str) -> None:
        nonlocal index
        indices[vertex] = index
        lowlinks[vertex] = index
        index += 1
        stack.append(vertex)
        on_stack.add(vertex)
        for neighbor in adjacency[vertex]:
            if neighbor not in indices:
                strongconnect(neighbor)
                lowlinks[vertex] = min(lowlinks[vertex], lowlinks[neighbor])
            elif neighbor in on_stack:
                lowlinks[vertex] = min(lowlinks[vertex], indices[neighbor])
        if lowlinks[vertex] == indices[vertex]:
            component: list[str] = []
            while True:
                node = stack.pop()
                on_stack.remove(node)
                component.append(node)
                if node == vertex:
                    break
            if len(component) > 1:
                components.append(component)

    for module_name in modules:
        if module_name not in indices:
            strongconnect(module_name)

    assert not components, "Import cycles detected:\n" + "\n".join(
        " <-> ".join(sorted(component)) for component in components
    )


def test_llm_layer_is_independent() -> None:
    violations: list[str] = []
    llm_root = BACKEND_ROOT / "llm"
    for path in llm_root.rglob("*.py"):
        for lineno, module in _top_level_imports(path):
            if module.startswith("backend.") and not module.startswith("backend.llm"):
                rel = path.relative_to(BACKEND_ROOT.parent)
                violations.append(f"{rel}:{lineno} imports {module}")
    assert not violations, "LLM layer must be independent:\n" + "\n".join(violations)


def test_legacy_layers_are_absent() -> None:
    legacy = [
        name
        for name in ("application", "domain", "model_settings")
        if (BACKEND_ROOT / name).exists()
    ]
    assert not legacy, f"Legacy layers must not return: {legacy}"


def test_repository_ports_are_feature_local() -> None:
    shared_ports = ast.parse(
        (BACKEND_ROOT / "business" / "shared" / "ports.py").read_text(encoding="utf-8")
    )
    shared_protocols = {
        node.name for node in shared_ports.body if isinstance(node, ast.ClassDef)
    }
    assert shared_protocols == {"CommitterPort"}
    for feature in ("account", "runs", "schedules", "settings"):
        assert (BACKEND_ROOT / "business" / feature / "ports.py").is_file()


def test_main_is_a_thin_bootstrap_entrypoint() -> None:
    imports = _top_level_imports(BACKEND_ROOT / "main.py")
    assert imports == [(3, "backend.bootstrap.app_factory")]


def test_llm_client_delegates_provider_specific_streaming() -> None:
    client = (BACKEND_ROOT / "llm" / "client.py").read_text(encoding="utf-8")
    assert "OPENAI_CHAT_COMPLETIONS" not in client
    assert "_stream_openai" not in client


def test_runtime_config_does_not_hold_llm_client() -> None:
    config = (BACKEND_ROOT / "agent" / "kernel" / "runtime_config.py").read_text(
        encoding="utf-8"
    )
    assert "llm_client:" not in config


def test_llm_tool_definition_has_no_agent_metadata() -> None:
    contracts = (BACKEND_ROOT / "llm" / "contracts.py").read_text(encoding="utf-8")
    tool_contract = contracts.split("class LLMToolDefinition", maxsplit=1)[1].split(
        "class LLMChatMessage", maxsplit=1
    )[0]
    assert "group" not in tool_contract
    assert "enabled_stages" not in tool_contract
    assert "side_effect_level" not in tool_contract


def test_no_oversized_handwritten_modules() -> None:
    """Hand-written app modules must stay under 1000 lines."""

    static_manifests: set[Path] = set()
    oversized: list[str] = []
    for path in BACKEND_ROOT.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        # Generated/vendor-ish paths are not present under app/, but keep the door open.
        if "migrations" in path.parts:
            continue
        if path in static_manifests:
            continue
        lines = sum(1 for _ in path.open(encoding="utf-8"))
        if lines > 1000:
            rel = path.relative_to(BACKEND_ROOT.parent)
            oversized.append(f"{rel}: {lines} lines")
    assert not oversized, "Modules exceed 1000 lines:\n" + "\n".join(oversized)


def test_api_routers_avoid_response_model_any() -> None:
    """JSON routers should not declare response_model=Any."""

    violations: list[str] = []
    api_root = BACKEND_ROOT / "api"
    for path in api_root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        if "response_model=Any" in text or "response_model = Any" in text:
            rel = path.relative_to(BACKEND_ROOT.parent)
            violations.append(str(rel))
    assert not violations, "response_model=Any found in:\n" + "\n".join(violations)
