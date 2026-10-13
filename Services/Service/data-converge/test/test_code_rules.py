"""Keep repository functions small without adding a lint dependency."""

import ast
from pathlib import Path


def test_python_functions_do_not_exceed_50_lines():
    service = Path(__file__).resolve().parents[1]
    violations = []
    for directory in (service / 'src', service / 'test'):
        for path in sorted(directory.rglob('*.py')):
            tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                start = min([node.lineno] + [item.lineno for item in node.decorator_list])
                length = node.end_lineno - start + 1
                if length > 50:
                    violations.append(f'{path.relative_to(service)}:{start} '
                                      f'{node.name}: {length} lines')
    assert not violations, '\n' + '\n'.join(violations)
