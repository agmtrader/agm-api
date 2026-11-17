#!/usr/bin/env python3
"""Copy the connector into the source of the existing AGM local marketplace."""
import argparse
import json
import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT = (Path.home() / "Library/Mobile Documents/com~apple~CloudDocs"
           / "Codex/Marketplaces/agm/plugins/agm")


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
    existing.setdefault("mcpServers", {}).update(config["mcpServers"])
    write_json(mcp_path, existing)
    manifest["mcpServers"] = "./.mcp.json"
    if manifest.get("version") == "0.1.5":
        manifest["version"] = "0.2.0"
    interface = manifest.get("interface", {})
    note = " Includes the AGM API connector for documented routes using your AGM account."
    for container, field in ((manifest, "description"), (interface, "longDescription")):
        if isinstance(container.get(field), str) and note not in container[field]:
            container[field] += note
    write_json(manifest_path, manifest)
    print(f"Updated AGM plugin source: {root}")


if __name__ == "__main__":
    main()
