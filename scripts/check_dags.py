import ast
import pathlib
import sys


def check_file(path: pathlib.Path) -> bool:
    try:
        ast.parse(path.read_text(encoding="utf-8"))
        print(f"OK: {path}")
        return True
    except SyntaxError as e:
        print(f"SYNTAX ERROR: {path}: {e}")
        return False


def main() -> int:
    root = pathlib.Path(__file__).resolve().parents[1]
    dags_dir = root / "dags"
    ok = True
    for py in dags_dir.glob("*.py"):
        ok = check_file(py) and ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())


