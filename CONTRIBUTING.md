# Contributing

Thank you for helping improve the Solar PV Asset Database.

## Development setup

The project uses Python 3.13 or later, SQLite, and pytest.

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
py -m pip install -e ".[dev]"
```

## Before submitting a change

Run both checks from the repository root:

```powershell
py -m pytest -v
py scripts/verify_week3_acceptance.py
```

Update the documentation when a change affects database fields, accepted data formats, import behavior, or setup instructions.

## Data rules

- Keep each inspection as a separate historical event.
- Preserve units, data origin, data-quality labels, and source references.
- Do not treat missing values as zero.
- Keep demonstration data clearly marked as synthetic.
- Do not commit private research data, credentials, personal information, or local database files.

## Pull requests

Use a focused branch and a clear commit message. Describe what changed, why it was needed, and which tests were run. Include relevant test output when changing data handling or import behavior.

Do not describe a feature as complete until it is implemented and verified.
