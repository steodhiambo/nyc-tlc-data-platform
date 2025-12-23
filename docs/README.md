# Documentation for NYC TLC Data Platform

This directory contains comprehensive documentation for the NYC TLC Data Platform.

## Available Documentation

- `comprehensive_documentation.md` - Complete system documentation with architecture diagrams and deployment instructions
- `troubleshooting_guide.md` - Detailed troubleshooting guide for common issues
- `architecture_diagrams.md` - Additional architecture diagrams and visualizations

## Generating Screenshots

To generate screenshots of the running system:

1. Start the platform: `docker-compose up -d`
2. Access the web interfaces:
   - Airflow: http://localhost:8080
   - Grafana: http://localhost:3000
   - Prometheus: http://localhost:9090
3. Take screenshots of the key dashboards and interfaces
4. Add them to this directory with descriptive names

## Architecture Diagrams

The documentation includes several Mermaid diagrams that can be rendered in compatible viewers or converted to images using online tools like:
- Mermaid Live Editor
- Mermaid CLI
- Various online diagram-to-image converters