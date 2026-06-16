import ast
import html
import inspect
from dataclasses import dataclass, field
from flask import Flask
from typing import Any, Iterable


@dataclass
class Param:
    name: str
    required: bool = False
    default: str | None = None


@dataclass
class RouteDocs:
    description: str
    query_params: dict[str, Param] = field(default_factory=dict)
    body_params: dict[str, Param] = field(default_factory=dict)
    has_docstring: bool = False


def _literal_string(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _display_default(node: ast.AST | None) -> str | None:
    if node is None:
        return None
    try:
        value = ast.literal_eval(node)
    except Exception:
        return None
    return repr(value)


def _record_param(params: dict[str, Param], name: str, required: bool = False, default: str | None = None) -> None:
    existing = params.get(name)
    if existing is None:
        params[name] = Param(name=name, required=required, default=default)
        return
    existing.required = existing.required or required
    if existing.default is None and default is not None:
        existing.default = default


def _is_request_args_call(node: ast.Call, method_name: str) -> bool:
    return (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == method_name
        and isinstance(node.func.value, ast.Attribute)
        and node.func.value.attr == 'args'
        and isinstance(node.func.value.value, ast.Name)
        and node.func.value.value.id == 'request'
    )


def _is_payload_get_call(node: ast.Call) -> bool:
    return (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == 'get'
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == 'payload'
    )


def _is_payload_subscript(node: ast.Subscript) -> bool:
    return isinstance(node.value, ast.Name) and node.value.id == 'payload'


def _humanize_route(route: str, methods: list[str]) -> str:
    if route == '/':
        return 'Serves the API landing page'
    if route == '/docs':
        return 'Serves the generated API documentation page'
    return f"Handles {'/'.join(methods)} requests for `{route}`"


def _extract_route_docs(view_func: Any, route: str, methods: list[str]) -> RouteDocs:
    original = inspect.unwrap(view_func)
    docstring = inspect.getdoc(original)
    description = docstring or _humanize_route(route, methods)
    docs = RouteDocs(description=description, has_docstring=bool(docstring))

    try:
        source = inspect.getsource(original)
    except (OSError, TypeError):
        return docs

    try:
        tree = ast.parse(inspect.cleandoc(source))
    except SyntaxError:
        return docs

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if _is_request_args_call(node, 'get') or _is_request_args_call(node, 'getlist'):
                if not node.args:
                    continue
                name = _literal_string(node.args[0])
                if name is None:
                    continue
                default = _display_default(node.args[1]) if len(node.args) > 1 else None
                _record_param(docs.query_params, name, default=default)

            if _is_payload_get_call(node):
                if not node.args:
                    continue
                name = _literal_string(node.args[0])
                if name is None:
                    continue
                default = _display_default(node.args[1]) if len(node.args) > 1 else None
                _record_param(docs.body_params, name, default=default)

        if isinstance(node, ast.Subscript) and _is_payload_subscript(node):
            name = _literal_string(node.slice)
            if name is not None:
                _record_param(docs.body_params, name, required=True)

    return docs


def _format_params(params: dict[str, Param]) -> str:
    if not params:
        return ''

    items: list[str] = []
    for param in sorted(params.values(), key=lambda item: item.name):
        label = html.escape(param.name)
        details: list[str] = []
        if param.required:
            details.append('required')
        if param.default is not None:
            details.append(f'default: {html.escape(param.default)}')
        if details:
            label = f"{label} <span class=\"muted\">({', '.join(details)})</span>"
        items.append(f'<li><code>{label}</code></li>')
    return '<ul>' + ''.join(items) + '</ul>'


def _summarize_params(params: dict[str, Param], label: str) -> str | None:
    if not params:
        return None

    required = sorted(param.name for param in params.values() if param.required)
    optional = sorted(param.name for param in params.values() if not param.required)
    parts: list[str] = []
    if required:
        parts.append(f"required {label}: {', '.join(required)}")
    if optional:
        parts.append(f"optional {label}: {', '.join(optional)}")
    return '; '.join(parts)


def _compose_description(
    route: str,
    methods: list[str],
    base_description: str,
    query_params: dict[str, Param],
    body_params: dict[str, Param],
    is_public: bool,
    use_base_only: bool,
) -> str:
    if use_base_only:
        return base_description.strip()

    lines = [base_description.rstrip('.')]
    lines.append('Public endpoint.' if is_public else 'JWT authentication required.')

    if 'GET' in methods or 'DELETE' in methods:
        query_summary = _summarize_params(query_params, 'query parameters')
        if query_summary:
            lines.append(query_summary)

    if any(method in methods for method in ('POST', 'PATCH', 'PUT')):
        body_summary = _summarize_params(body_params, 'JSON body fields')
        if body_summary:
            lines.append(body_summary)

    return ' '.join(lines).strip() + '.'


def _format_description(description: str) -> str:
    escaped = html.escape(description)
    return escaped.replace('\n', '<br>')


def generate_docs(
    app: Flask,
    output_path: str = 'public/static/docs.html',
    skip_endpoints: Iterable[str] | None = None,
    public_endpoints: Iterable[str] | None = None,
) -> None:
    """Generate a basic HTML documentation file listing all registered routes.

    Args:
        app (Flask): The flask application instance.
        output_path (str, optional): File path where the documentation will be written. Defaults to 'public/static/docs.html'.
        skip_endpoints (Iterable[str] | None, optional): Endpoints that should be excluded from docs. Defaults to None.
        public_endpoints (Iterable[str] | None, optional): Endpoints that do not require JWT auth. Defaults to None.
    """
    if skip_endpoints is None:
        skip_endpoints = {'static'}
    if public_endpoints is None:
        public_endpoints = set()
    else:
        public_endpoints = set(public_endpoints)

    rows: list[tuple[str, str, str, str, str]] = []
    for rule in app.url_map.iter_rules():
        if rule.endpoint in skip_endpoints:
            continue
        # Extract allowed HTTP methods, ignoring implicit methods
        methods = sorted(m for m in rule.methods if m not in {'HEAD', 'OPTIONS'})
        method_str = ', '.join(methods)
        view_func = app.view_functions.get(rule.endpoint)
        route_docs = _extract_route_docs(view_func, str(rule.rule), methods) if view_func else RouteDocs(_humanize_route(str(rule.rule), methods))
        description = _compose_description(
            route=str(rule.rule),
            methods=methods,
            base_description=route_docs.description,
            query_params=route_docs.query_params,
            body_params=route_docs.body_params,
            is_public=rule.endpoint in public_endpoints,
            use_base_only=route_docs.has_docstring,
        )
        rows.append((
            str(rule.rule),
            method_str,
            description,
            _format_params(route_docs.query_params),
            _format_params(route_docs.body_params),
        ))

    rows.sort(key=lambda x: x[0])

    html_parts: list[str] = [
        '<!DOCTYPE html>',
        '<html lang="en">',
        '<head>',
        '  <meta charset="UTF-8">',
        '  <meta name="viewport" content="width=device-width, initial-scale=1.0">',
        '  <title>AGM API Documentation</title>',
        '  <style>',
        '    body { font-family: Arial, sans-serif; margin: 2rem; color: #202124; }',
        '    table { border-collapse: collapse; width: 100%; }',
        '    th, td { border: 1px solid #ccc; padding: 8px 12px; vertical-align: top; }',
        '    th { background: #f4f4f4; text-align: left; }',
        '    tr:nth-child(even) { background: #fafafa; }',
        '    code { font-family: Menlo, Consolas, monospace; font-size: 0.92em; }',
        '    ul { margin: 0; padding-left: 1.2rem; }',
        '    .route { white-space: nowrap; }',
        '    .methods { white-space: nowrap; }',
        '    .muted { color: #666; font-family: Arial, sans-serif; }',
        '  </style>',
        '</head>',
        '<body>',
        '  <h1>AGM API Routes</h1>',
        '  <table>',
        '    <tr><th>Route</th><th>Methods</th><th>Description</th><th>Query&nbsp;Parameters</th><th>JSON&nbsp;Body</th></tr>'
    ]

    for route, methods, description, query_params, body_params in rows:
        html_parts.append(
            '    <tr>'
            f'<td class="route"><code>{html.escape(route)}</code></td>'
            f'<td class="methods">{html.escape(methods)}</td>'
            f'<td>{_format_description(description)}</td>'
            f'<td>{query_params}</td>'
            f'<td>{body_params}</td>'
            '</tr>'
        )

    html_parts.extend([
        '  </table>',
        '</body>',
        '</html>'
    ])

    doc_html = '\n'.join(html_parts)

    # Write the generated documentation to the specified path
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(doc_html)
