from flask import Flask
from typing import Iterable


def generate_docs(app: Flask, output_path: str = 'public/static/docs.html', skip_endpoints: Iterable[str] | None = None) -> None:
    """Generate a basic HTML documentation file listing all registered routes.

    Args:
        app (Flask): The flask application instance.
        output_path (str, optional): File path where the documentation will be written. Defaults to 'public/static/docs.html'.
        skip_endpoints (Iterable[str] | None, optional): Endpoints that should be excluded from docs. Defaults to None.
    """
    if skip_endpoints is None:
        skip_endpoints = {'static'}

    rows: list[tuple[str, str, str]] = []
    for rule in app.url_map.iter_rules():
        if rule.endpoint in skip_endpoints:
            continue
        # Extract allowed HTTP methods, ignoring implicit methods
        methods = sorted(m for m in rule.methods if m not in {'HEAD', 'OPTIONS'})
        method_str = ', '.join(methods)
        params = ', '.join(sorted(rule.arguments)) if rule.arguments else ''
        rows.append((str(rule.rule), method_str, params))

    rows.sort(key=lambda x: x[0])

    html_parts: list[str] = [
        '<!DOCTYPE html>',
        '<html lang="en">',
        '<head>',
        '  <meta charset="UTF-8">',
        '  <meta name="viewport" content="width=device-width, initial-scale=1.0">',
        '  <title>AGM API Documentation</title>',
        '  <style>',
        '    body { font-family: Arial, sans-serif; margin: 2rem; }',
        '    table { border-collapse: collapse; width: 100%; }',
        '    th, td { border: 1px solid #ccc; padding: 8px 12px; }',
        '    th { background: #f4f4f4; text-align: left; }',
        '    tr:nth-child(even) { background: #fafafa; }',
        '  </style>',
        '</head>',
        '<body>',
        '  <h1>AGM API Routes</h1>',
        '  <table>',
        '    <tr><th>Route</th><th>Methods</th><th>Path&nbsp;Parameters</th></tr>'
    ]

    for route, methods, params in rows:
        html_parts.append(f'    <tr><td>{route}</td><td>{methods}</td><td>{params}</td></tr>')

    html_parts.extend([
        '  </table>',
        '</body>',
        '</html>'
    ])

    doc_html = '\n'.join(html_parts)

    # Write the generated documentation to the specified path
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(doc_html)
