"""Export the FastAPI OpenAPI document without starting app lifespan."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from backend.bootstrap.runtime_config import RuntimeConfig
from backend.main import create_app


def export_openapi() -> dict[str, Any]:
    app = create_app(
        RuntimeConfig(
            scheduler_enabled=False,
            serve_frontend=False,
        )
    )
    return app.openapi()


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Aniubot OpenAPI JSON.")
    parser.add_argument("--output", required=True, help="Destination JSON file path.")
    args = parser.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(export_openapi(), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
