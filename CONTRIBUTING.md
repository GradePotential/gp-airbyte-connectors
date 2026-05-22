# Contributing

Thanks for your interest in contributing. This project follows standard open source conventions.

## How to contribute

1. **Open an issue first** for anything beyond a trivial fix — describe the problem or feature so we can discuss the approach before you invest time writing code.

2. **Fork the repo** and create a branch from `main`:
   ```bash
   git checkout -b fix/your-description
   # or
   git checkout -b feat/your-description
   ```

3. **Make your change**, following the development workflow in the README.

4. **Open a pull request** against `main`. Fill in the description — what changed and why.

## Pull request expectations

- PRs require one approving review before merge.
- Keep changes focused. One logical change per PR makes review easier and keeps history readable.
- If your change modifies the connector's output schema (adds or renames fields), note it clearly in the PR description — it has downstream effects on destination tables.

## Reporting bugs

Open a GitHub issue with:
- Which connector (`source-mssql-ct` or `source-mongodb-v2`)
- What you expected vs. what happened
- Relevant Airbyte job logs if available

## Questions

Open a discussion or issue — we're happy to help with setup or usage questions.
