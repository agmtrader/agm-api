#!/usr/bin/env python3
"""Copy the connector into the source of the existing AGM local marketplace."""
import argparse
import json
import shutil
from pathlib import Path

from agm_mcp import VERSION

HERE = Path(__file__).resolve().parent
DEFAULT = Path.home() / "Library/Mobile Documents/com~apple~CloudDocs/Codex/Marketplaces/agm/plugins/agm"


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plugin-dir", type=Path, default=DEFAULT)
    args = parser.parse_args()
    root = args.plugin_dir.expanduser().resolve()
    manifest_path = root / ".codex-plugin/plugin.json"
    mcp_path = root / ".mcp.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("name") != "agm":
        parser.error("Destination is not the existing AGM plugin.")
    if manifest.get("mcpServers") not in (None, "./.mcp.json"):
        parser.error("The AGM manifest already points to a different MCP configuration.")
    existing = json.loads(mcp_path.read_text(encoding="utf-8")) if mcp_path.exists() else {}
    config = json.loads((HERE / ".mcp.json").read_text(encoding="utf-8"))
    destination = root / "mcp"
    destination.mkdir(exist_ok=True)
    for name in ("agm_mcp.py", "launch"):
        shutil.copy2(HERE / name, destination / name)
    shutil.copyfile(HERE.parent.parent / "public/static/docs.html", destination / "docs.html")
    shutil.copyfile(HERE / "README.md", destination / "README.md")
    # API usage guidance is supplied by the MCP server, not a separate skill.
    retired_skill = root / "skills/agm-api/SKILL.md"
    retired_skill.unlink(missing_ok=True)
    if retired_skill.parent.is_dir() and not any(retired_skill.parent.iterdir()):
        retired_skill.parent.rmdir()
    servers = existing.setdefault("mcpServers", {})
    for legacy_name in ("agm-api", "Agm-api"):
        servers.pop(legacy_name, None)
    servers.update(config["mcpServers"])
    write_json(mcp_path, existing)
    manifest["mcpServers"] = "./.mcp.json"
    if manifest.get("version") in {"0.1.4", "0.1.5", "0.2.0", "0.3.0", "0.3.1", "0.3.2", "0.3.3", "0.3.4", "0.3.5", "0.3.6", "0.3.7", VERSION}:
        manifest["version"] = VERSION
    interface = manifest.get("interface", {})
    old_note = " Includes the AGM API connector for documented routes using your AGM account."
    note = " Includes a read-only AGM API connector that uses your AGM account."
    for container, field in ((manifest, "description"), (interface, "longDescription")):
        if isinstance(container.get(field), str) and note not in container[field]:
            container[field] = container[field].replace(old_note, "") + note
    write_json(manifest_path, manifest)
    print(f"Updated AGM plugin source: {root}")


if __name__ == "__main__":
    main()
