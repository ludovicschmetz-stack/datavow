# datavow

## Project layout

- `src/datavow/` — main package (src layout)
- `tests/` — pytest test suite
- CLI built with Typer (`src/datavow/cli.py`)

## Development

```bash
pip install -e '.[dev]'
pytest
```

## Conventions

- Python >=3.10, use modern syntax (match, `X | Y` unions, etc.)
- CLI commands live in `src/datavow/cli.py` using Typer
- Tests use pytest; prefer `tmp_path` fixture for filesystem tests
- Keep dependencies minimal
