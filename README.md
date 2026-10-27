# Solar PV Asset Database

A Python application for storing solar asset information, keeping inspection history, and tracking changes in asset conditions over time.

## Project Goals

- Store information about solar assets such as panels, rows, and inverters.
- Keep a separate record for each inspection.
- Store inspection measurements and findings with their units and data quality.
- Record maintenance activities.
- Import inspection data from CSV and JSON files.
- View asset history and condition trends through a dashboard.
- Identify possible worsening conditions using clear, explainable rules.

## Planned Technology

- Python
- SQLite for the initial database
- Streamlit for the dashboard
- Plotly for charts
- Git and GitHub for version control
- Automated tests for reliability

## Project Status

**Stage:** Planning and initial setup.

The database design and data format will be agreed upon before implementation begins. Features will be marked complete only after they have been implemented and tested.

## Development Approach

We aim to keep the code simple, readable, and maintainable while protecting historical records and validating imported data.

Each change will be reviewed and tested before it is committed and pushed to GitHub.

## Research Data

Real research data, private files, credentials, and local database files must not be committed to the public repository. Only approved sample data should be shared.