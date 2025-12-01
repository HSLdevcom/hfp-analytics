# HFP Analytics API Development Container

This devcontainer provides a complete development environment for the HFP Analytics FastAPI application.

## What's Included

- **Python 3.10** with all project dependencies
- **Azure Functions Core Tools** for Azure development (AMD64 only)
- **PostgreSQL client** for database operations
- **TimescaleDB** database instance
- **Azurite** for local Azure Storage emulation
- **VS Code extensions** for Python, Azure, Docker, and more
- **ARM64 (Apple Silicon) support** - works on M1/M2/M3 Macs

## Getting Started

1. **Open in Dev Container**
   - Open this folder in VS Code
   - Press `F1` and select "Dev Containers: Reopen in Container"
   - Wait for the container to build and start

2. **Run the FastAPI App**
   ```bash
   cd python
   uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
   ```
   
   Or use the VS Code task: `Terminal > Run Task > Run FastAPI Development Server`

3. **Access the Application**
   - FastAPI: http://localhost:8000
   - API Docs: http://localhost:8000/docs
   - PostgreSQL: localhost:5433 (user: postgres, password: postgres)
   - Azurite Blob: http://localhost:10100
   - Azurite Queue: http://localhost:10101
   - Azurite Table: http://localhost:10102

## Debugging

Use the built-in debugger:
1. Set breakpoints in your code
2. Press `F5` or go to "Run and Debug"
3. Select "Python: FastAPI" configuration

## Database Setup

Initialize the database schema:
```bash
psql -h localhost -U postgres -d analytics -f db/sql/100_create_global_objects.sql
```

Or use the VS Code task: `Terminal > Run Task > Database: Run Migrations`

## Running Tests

```bash
cd python
pytest
```

Or use the VS Code task: `Terminal > Run Task > Run Tests`

## Tips

- All changes to Python files trigger auto-reload
- Use `.env` file for environment variables
- The workspace is mounted at `/workspace`
- Extensions and settings are pre-configured

## Ports

| Service | Port | Description |
|---------|------|-------------|
| FastAPI | 8000 | Main API server |
| PostgreSQL | 5433 | TimescaleDB database (remapped to avoid conflicts) |
| Azurite Blob | 10100 | Azure Blob Storage emulator (remapped) |
| Azurite Queue | 10101 | Azure Queue Storage emulator (remapped) |
| Azurite Table | 10102 | Azure Table Storage emulator (remapped) |

## Troubleshooting

**Container won't start:**
- Make sure Docker is running
- Check if ports 5433, 8000, 10100, 10101, 10102 are available
- Port 5433 is used instead of 5432 to avoid conflicts with local PostgreSQL
- Ports 10100-10102 are used for Azurite to avoid conflicts with main docker-compose
- Try rebuilding: `F1 > Dev Containers: Rebuild Container`

**Database connection issues:**
- Wait for the database health check to pass (about 10-15 seconds)
- Check connection string in `.env` file

**Import errors:**
- Make sure PYTHONPATH is set correctly: `export PYTHONPATH=/workspace/python`
- Reinstall dependencies: `pip install -r python/requirements.txt`
