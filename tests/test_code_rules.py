"""Keep repository functions small without adding a lint dependency."""

import ast

from .paths import REPOSITORY_ROOT, SOURCE_ROOT


def test_python_functions_do_not_exceed_50_lines():
    violations = []
    for directory in (SOURCE_ROOT, REPOSITORY_ROOT / 'tests'):
        for path in sorted(directory.rglob('*.py')):
            tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                start = min([node.lineno] + [item.lineno for item in node.decorator_list])
                length = node.end_lineno - start + 1
                if length > 50:
                    violations.append(f'{path.relative_to(REPOSITORY_ROOT)}:{start} '
                                      f'{node.name}: {length} lines')
    assert not violations, '\n' + '\n'.join(violations)
