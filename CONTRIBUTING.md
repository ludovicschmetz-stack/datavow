# Contributing to DataVow

Thanks for your interest in DataVow! Here's how to get started.

## Development setup

```bash
git clone https://github.com/ludovicschmetz-stack/datavow.git
cd datavow
python -m venv .venv && source .venv/bin/activate
uv pip install -e ".[dev]"
```

## Running tests

```bash
pytest
```

All 137 tests should pass. CI runs on Python 3.12 and 3.13.

## Linting

```bash
ruff check src/ tests/
ruff format src/ tests/
```

## Submitting changes

1. Fork the repo and create a branch from `main`
2. Add tests for any new functionality
3. Run `pytest` and `ruff check` — both must pass
4. Open a pull request with a clear description

## Reporting bugs

Open an [issue](https://github.com/ludovicschmetz-stack/datavow/issues/new?template=bug_report.md) with:
- DataVow version (`datavow --version`)
- Python version
- Minimal contract + data to reproduce
- Expected vs actual behavior

## Feature requests

Open an [issue](https://github.com/ludovicschmetz-stack/datavow/issues/new?template=feature_request.md) describing:
- The problem you're trying to solve
- How you'd expect it to work
- Why existing features don't cover it

## Code of conduct

Be respectful, constructive, and professional. We're building infrastructure that teams rely on.

## License

By contributing, you agree that your contributions will be licensed under Apache 2.0.
