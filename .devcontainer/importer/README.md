# HFP Analytics Importer Development Container

This devcontainer provides a complete development environment for the HFP Analytics Azure Functions (Importer) application.

## What's Included

- **Python 3.10** with all project dependencies
- **Azure Functions Core Tools v4** for local Azure Functions development
- **PostgreSQL client** for database operations
- **TimescaleDB** database instance
- **Azurite** for local Azure Storage emulation
- **VS Code extensions** for Python, Azure Functions, Docker, and more
- **ARM64 (Apple Silicon) support** - works on M1/M2/M3 Macs

## Getting Started

1. **Open in Dev Container**
   - Open the `.devcontainer/importer` folder in VS Code
   - Press `F1` and select "Dev Containers: Reopen in Container"
   - Wait for the container to build and start

2. **Start Azure Functions**
   
   **Method 1: Using VS Code Task (Recommended)**
   - Press `Ctrl+Shift+P` (or `Cmd+Shift+P` on Mac)
   - Select `Tasks: Run Task`
   - Choose `Start Azure Functions`
   
   **Method 2: Using Terminal**
   ```bash
   cd python
   func start --port 7071
   ```
   
   **Method 3: Using the built-in command**
   ```bash
   cd python
   func host start --port 7071
   ```

3. **Access the Application**
   - Azure Functions: http://localhost:7071
   - Function Admin: http://localhost:7071/admin/functions
   - PostgreSQL: localhost:5433 (user: postgres, password: postgres) - shared with API
   - Azurite Blob: http://localhost:10100 - shared with API
   - Azurite Queue: http://localhost:10101 - shared with API
   - Azurite Table: http://localhost:10102 - shared with API

## Available Functions

The importer includes these Azure Functions:
- **httpPreprocess** - HTTP-triggered preprocessing function
- **importer** - Main import function
- **analyzer** - Analysis function
- **preprocess** - Preprocessing function
- **httpStart** (Durable) - Durable orchestration starter
- **orchestrator** (Durable) - Durable orchestrator
- **reclusterAnalysisActivity** (Durable) - Recluster analysis activity
- **setStatusActivity** (Durable) - Status setter activity
- **getStatusActivity** (Durable) - Status getter activity

## Testing Functions

### Test HTTP Function
```bash
# Test httpPreprocess
curl -X POST http://localhost:7071/httpPreprocess

# Or use VS Code task:
# Ctrl+Shift+P -> Tasks: Run Task -> Trigger HTTP Function (httpPreprocess)
```

### View Function Logs
Azure Functions logs appear in the terminal where you ran `func start`.

## Debugging

### Debug Azure Functions:
1. Start Azure Functions with the task or terminal command
2. Set breakpoints in your Python code
3. Use `F5` or go to "Run and Debug"
4. Select "Attach to Python Functions"
5. Trigger your function via HTTP, timer, or queue

### Debug Current File:
1. Open a Python file
2. Set breakpoints
3. Press `F5`
4. Select "Python: Current File"

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

## Azure Functions Development Tips

1. **Hot Reload**: Azure Functions automatically reload when you save Python files

2. **View Function List**:
   ```bash
   cd python
   func list
   ```

3. **Check Function Status**:
   ```bash
   curl http://localhost:7071/admin/functions
   ```

4. **Environment Variables**: Configure in `.env` file at workspace root

5. **Function Configuration**: Each function has a `function.json` in its folder

6. **Host Configuration**: Global settings in `python/host.json`

## Ports

| Service | Port | Description |
|---------|------|-------------|
| Azure Functions | 7071 | Main Functions host |
| PostgreSQL | 5433 | TimescaleDB database (shared with API) |
| Azurite Blob | 10100 | Azure Blob Storage emulator (shared with API) |
| Azurite Queue | 10101 | Azure Queue Storage emulator (shared with API) |
| Azurite Table | 10102 | Azure Table Storage emulator (shared with API) |

## Troubleshooting

**Container won't start:**
- Make sure Docker is running
- Check if ports 5433, 7071, 10100, 10101, 10102 are available
- Database and Azurite are shared with the API container
- Ports are remapped to avoid conflicts with main docker-compose
- Try rebuilding: `F1 > Dev Containers: Rebuild Container`

**Functions won't start:**
- Verify you're in the `python` directory: `cd python`
- Check `host.json` exists in `python/` folder
- Ensure all function folders have `function.json`
- Check Azure Functions Core Tools: `func --version`

**Database connection issues:**
- Wait for the database health check to pass (about 10-15 seconds)
- Check connection string in `.env` file
- Use port 5434 (not 5432) from host machine

**Import errors:**
- Make sure PYTHONPATH is set: `export PYTHONPATH=/workspace/python`
- Reinstall dependencies: `pip install -r python/requirements.txt`

**Storage connection errors:**
- Check Azurite is running: `docker ps | grep azurite`
- Verify storage connection string in `.env` uses correct ports (10100-10102)
- Default connection string: `DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10100/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10101/devstoreaccount1;TableEndpoint=http://127.0.0.1:10102/devstoreaccount1;`

## Project Structure

```
python/
├── host.json                 # Azure Functions host configuration
├── requirements.txt          # Python dependencies
├── importer/                 # Importer function
│   ├── function.json
│   └── __init__.py
├── analyzer/                 # Analyzer function
├── preprocess/              # Preprocess function
├── httpPreprocess/          # HTTP preprocess function
├── durable/                 # Durable functions
│   ├── httpStart/
│   ├── orchestrator/
│   └── *Activity/
└── common/                  # Shared code
```

## Useful Commands

```bash
# List all functions
cd python && func list

# Start with verbose logging
cd python && func start --port 7071 --verbose

# Install new dependency
pip install <package> && pip freeze > python/requirements.txt

# Run specific test file
cd python && pytest tests/test_importer.py -v

# Check Azure Functions version
func --version

# View function templates
func templates list
```
