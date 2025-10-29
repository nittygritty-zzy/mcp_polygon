#!/usr/bin/env python3
"""Add necessary imports to all tool files."""

from pathlib import Path

# New imports to add
new_imports = '''from ..clients import poly_mcp, polygon_client
from ..formatters import json_to_csv
import json

'''

tools_dir = Path("src/mcp_polygon/tools")

for tool_file in tools_dir.glob("*.py"):
    if tool_file.name == "__init__.py":
        continue

    content = tool_file.read_text()

    # Insert new imports after the existing imports
    lines = content.split('\n')

    # Find the line after the last import
    insert_pos = 0
    for i, line in enumerate(lines):
        if line.startswith('from datetime import'):
            insert_pos = i + 1
            break

    # Insert the new imports
    lines.insert(insert_pos, new_imports)

    # Write back
    tool_file.write_text('\n'.join(lines))
    print(f"✓ Updated {tool_file.name}")

print("\nAll tool files updated!")
