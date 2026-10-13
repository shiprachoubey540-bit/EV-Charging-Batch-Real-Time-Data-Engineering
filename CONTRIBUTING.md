# Contributing Guide

Thank you for contributing to the EV Charging Data Engineering project! This document explains our branching strategy, commit conventions, and pull request workflow.

---

## Branching Strategy

We use **feature branches** off `main`. Never push directly to `main`.

### Branch Naming Convention

`feature/<module>-<short-description>`

| Owner | Example Branch |
|-------|-------------|
| Ananthan S | `feature/ingestion-csv-loader` |
| Harikrishnan SM | `feature/streaming-kafka-producer` |
| Aleena Antony | `feature/processing-batch-transform` |
| Bainty Kaur | `feature/database-schema-design` |
| Shipra Choubey | `feature/orchestration-dag-setup` |

For bug fixes: `fix/<module>-<short-description>`
For documentation: `docs/<short-description>`

---

## Commit Messages

Use clear, descriptive commit messages:

`<type>(<module>): <short summary>`

**Types:** `feat`, `fix`, `docs`, `test`, `refactor`, `chore`

**Examples:**

- `feat(ingestion): add CSV extraction from ACN-Data`
- `fix(streaming): handle missing timestamp in events`
- `docs(database): add data dictionary for session table`
- `test(processing): add unit tests for batch transforms`

---

## Pull Request Workflow

1. Create a feature branch from an up-to-date `main`.
2. Develop and commit your changes with clear messages.
3. Push your branch and open a Pull Request on GitHub.
4. Assign at least one reviewer (preferably from a dependent module).
5. Address review feedback and push fixes to the same branch.
6. Merge only after at least one approval. Use Squash and merge.

---

## Code Review Checklist

- Code runs without errors
- Follows the shared data contracts in `contracts/data_contracts.yaml`
- No secrets, API keys, or `.env` files committed
- New functionality has corresponding tests
- Documentation is updated if applicable
- No large data files (> 1 MB) are committed

---

## Module Boundaries

Each team member owns specific directories. When your work requires changes in another module, discuss with the module owner first and create a cross-module PR tagging both owners as reviewers.

---

## Setting Up Your Development Environment

1. Clone the repo and create a virtual environment.
2. Install dependencies: `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and fill in your local settings.

See `docs/setup_guide.md` for detailed instructions.
