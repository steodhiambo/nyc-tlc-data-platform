# Contributing to NYC TLC Data Platform

Thank you for your interest in contributing to the NYC TLC Data Platform! This document outlines the guidelines for contributing to this project.

## Getting Started

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Commit your changes (`git commit -m 'Add amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

## Development Environment

### Setting Up Locally

1. Clone your forked repository:
```bash
git clone https://github.com/YOUR_USERNAME/nyc-tlc-data-platform.git
cd nyc-tlc-data-platform
```

2. Install dependencies and set up environment:
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Code Standards

### Python
- Follow PEP 8 style guide
- Use type hints for all function signatures
- Write docstrings for all modules, classes, and functions
- Maximum line length of 100 characters

### SQL
- Use uppercase for SQL keywords (SELECT, FROM, WHERE, etc.)
- Use lowercase for table and column names
- Format queries with proper indentation

### Airflow DAGs
- Use meaningful task IDs
- Set appropriate retries and timeout values
- Include data quality checks
- Document complex transformations

## Testing

### Unit Tests
Write unit tests for all transformation functions:
```bash
pytest tests/unit/
```

### Integration Tests
Test DAG logic and data flow:
```bash
pytest tests/integration/
```

### End-to-End Tests
Validate the complete pipeline:
```bash
pytest tests/e2e/
```

## Documentation

Update the following documentation when making changes:
- README.md for installation and usage
- docs/architecture.md for system architecture
- DAG docstrings for pipeline logic
- Code comments for complex implementations

## Branch Naming Convention

Use the following prefixes for branch names:
- `feature/` for new features
- `bugfix/` for bug fixes
- `hotfix/` for urgent fixes
- `refactor/` for refactoring tasks
- `docs/` for documentation updates

## Pull Request Guidelines

1. Describe the changes made in the PR description
2. Link to any relevant issues
3. Include test results
4. Update documentation as needed
5. Ensure all CI checks pass

## Code Review Process

1. Submit pull request with clear description
2. Assign reviewers (minimum 1 approval required)
3. Address review comments
4. Merge after approval

## Reporting Issues

When reporting issues, please include:
- Clear title and description
- Steps to reproduce
- Expected vs actual behavior
- Environment details (OS, Python version, etc.)

## Questions?

Feel free to reach out via the issue tracker for any questions about contributing.