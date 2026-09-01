#!/usr/bin/env python3
"""Parse static YAML files and perform a few repository-specific invariants."""
from __future__ import annotations

import sys
from pathlib import Path

try:
    import yaml
except ImportError as exc:
    raise SystemExit("PyYAML is required: python3 -m pip install pyyaml") from exc

ROOT = Path(__file__).resolve().parents[1]
FILES = [
    ROOT / "compose.local.yml",
    ROOT / "compose.remote.yml",
    ROOT / "compose.observability.yml",
    ROOT / "compose.test.yml",
    ROOT / "docs/openapi.yaml",
    ROOT / ".github/workflows/ci.yml",
]

errors: list[str] = []
for path in FILES:
    if not path.exists():
        errors.append(f"missing {path.relative_to(ROOT)}")
        continue
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        errors.append(f"{path.relative_to(ROOT)}: {exc}")
        continue
    if not isinstance(doc, dict):
        errors.append(f"{path.relative_to(ROOT)}: top-level document must be a mapping")

local = yaml.safe_load((ROOT / "compose.local.yml").read_text(encoding="utf-8"))
for service in ("gateway", "postgres", "redis", "kafka", "ollama"):
    if service not in local.get("services", {}):
        errors.append(f"compose.local.yml: missing service {service}")

openapi = yaml.safe_load((ROOT / "docs/openapi.yaml").read_text(encoding="utf-8"))
if openapi.get("openapi") != "3.1.0":
    errors.append("docs/openapi.yaml: expected OpenAPI 3.1.0")

if errors:
    print("\n".join(errors), file=sys.stderr)
    raise SystemExit(1)
print(f"validated {len(FILES)} YAML files")
