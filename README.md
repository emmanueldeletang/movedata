# Azure Cosmos DB MongoDB Migration Tool

This application connects to Azure Cosmos DB for MongoDB and provides the following functionality:

- Connect to source and destination Azure Cosmos DB MongoDB accounts
- List databases and collections in the source
- Select and copy databases or collections from source to destination
- Enable and manage change streams for real-time data replication
- **NEW**: PostgreSQL schema and data migration between Azure PostgreSQL databases

## Features

### MongoDB Migration
- **Dual Interface**: Both Command-Line Interface (CLI) and modern Windows GUI
- Connection to Azure Cosmos DB MongoDB API
- Database and collection listing with document counts
- Data migration between MongoDB accounts with progress tracking
- Change stream replication for real-time data synchronization

### PostgreSQL Migration (NEW!)
- **Azure PostgreSQL Support**: Connect to Azure Database for PostgreSQL
- **Schema Migration**: Migrate table structures, indexes, and constraints
- **Data Migration**: Batch migration with progress tracking
- **Flexible Options**: Schema Only, Data Only, or Schema + Data migration
- **Robust Connection Management**: Connection pooling, retry logic, and health monitoring

### General Features
- Comprehensive error handling and logging
- Configuration file support for easy setup
- Modern Windows GUI with tabbed interface
- Background processing for non-blocking operations

## Requirements

- Python 3.8+
- Windows OS (for GUI features)
- Azure Cosmos DB MongoDB API accounts
- **Azure Database for PostgreSQL** (for PostgreSQL migration)
- Required Python packages (see requirements.txt)

## Installation

1. Clone this repository
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the root directory with the following settings:

### MongoDB Configuration
```
SOURCE_CONNECTION_STRING=mongodb://your-source-account:your-password@your-source-account.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@your-source-account@
DEST_CONNECTION_STRING=mongodb://your-destination-account:your-password@your-destination-account.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@your-destination-account@
```

### PostgreSQL Configuration (NEW!)
```
PG_SOURCE_CONNECTION_STRING=postgresql://user:pass@source-server.postgres.database.azure.com:5432/source_db
PG_DEST_CONNECTION_STRING=postgresql://user:pass@dest-server.postgres.database.azure.com:5432/dest_db
```

### Optional Performance Settings
```
BATCH_SIZE=1000
TIMEOUT_MS=30000
RETRY_ATTEMPTS=3
RETRY_DELAY_MS=1000
CONNECTION_RETRY_ATTEMPTS=5
CONNECTION_RETRY_DELAY_MS=2000
OPERATION_RETRY_ATTEMPTS=3
HEALTH_CHECK_INTERVAL_SECONDS=30
```

Alternatively, you can use managed identity authentication if your application is running in an Azure environment that supports it.

## Usage

### Windows GUI Mode (Recommended)

For the best user experience on Windows, use the GUI interface:

**Option 1: Using the batch launcher (simplest)**
```bash
launch_gui.bat
```

**Option 2: Direct Python execution**
```bash
python gui_launcher.py
```

**Option 3: Through main module**
```bash
python src/main.py --gui
```

### Command-Line Interface Mode

For server environments or scripted operations:

```bash
python src/main.py
```

### GUI Features

The Windows GUI provides:

- **Connection Tab**: 
  - Secure connection string input with masking
  - Connection testing and validation
  - Configuration file loading (.env files)
  - Real-time connection status display
  - Interactive database and collection browser

- **Migration Tab**:
  - Choice between full database or selective collection migration
  - Visual collection selection with document counts
  - Real-time progress tracking with statistics
  - Configurable batch sizes
  - Migration time estimation and speed monitoring

- **PostgreSQL Migration Tab** (NEW!):
  - **Connection Management**: Secure PostgreSQL connection string input
  - **Schema Browsing**: Tree view of schemas, tables, and estimated row counts
  - **Migration Options**: Choose between Schema Only, Data Only, or Schema + Data
  - **Flexible Scope**: Migrate individual tables, entire schemas, or all schemas
  - **Progress Tracking**: Real-time progress bars and migration statistics
  - **Background Processing**: Non-blocking migration operations



- **Logs Tab**:
  - Real-time log display with configurable log levels
  - Log filtering and search capabilities
  - Export logs to files for analysis
  - Integrated error reporting and diagnostics



This will:
1. Connect to your configured databases
2. Start a change stream on an available collection
3. Provide instructions for manual testing
4. Show detailed logs of all detected changes
5. Display comprehensive change information including complete document content

The test script demonstrates the enhanced visibility features:
- 🔄 Real-time change detection with operation type indicators
- 📄 Complete document logging for all change types
- ✅ Replication success confirmation
- 📊 Processing statistics and performance metrics

### VS Code Integration

Use the integrated tasks for development:

- **Ctrl+Shift+P** → "Tasks: Run Task"
  - "Run Migration Tool" - CLI mode
  - "Run Migration Tool (GUI)" - GUI mode
  - "Run Migration Tool (CLI with GUI option)" - CLI with GUI flag


## Security Best Practices

- Connection strings are masked in the GUI for security
- Supports Azure Managed Identity for credential-free authentication
- Logs exclude sensitive connection information
- Configuration files should be excluded from version control

## Troubleshooting

### Common Issues

1. **GUI won't start**: Ensure Python 3.8+ is installed and tkinter is available
2. **Connection failures**: Verify connection strings and network connectivity
3. **Performance issues**: Adjust batch size settings for your environment


### Getting Help

```bash
python src/main.py --help
```

## Development

### Project Structure

```
cosmosdbongo/
├── src/
│   ├── main.py              # Main entry point with CLI/GUI selection
│   ├── gui.py               # Windows GUI implementation
│   ├── cli.py               # Command-line interface
│   ├── connection_manager.py # Database connection management
│   ├── migration_service.py  # Core migration logic
│   └── config.py            # Configuration management
├── gui_launcher.py          # Direct GUI launcher
├── launch_gui.bat          # Windows batch launcher
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## License

MIT
