# PostgreSQL Migration Tab - User Guide

## Overview

The PostgreSQL Migration tab has been successfully added to the Azure Cosmos DB MongoDB Migration Tool GUI. This new tab provides comprehensive functionality for migrating schemas and data between two Azure PostgreSQL databases.

## Features

### 🔌 **Connection Management**
- **Robust Connection Handling**: Implements connection pooling and retry logic
- **Health Monitoring**: Continuous health checks for both source and destination databases
- **Azure Integration**: Supports Azure Database for PostgreSQL with proper authentication

### 🗄️ **Schema & Data Migration**
- **Schema Migration**: Migrate table structures, indexes, constraints, and relationships
- **Data Migration**: Batch migration of table data with progress tracking
- **Flexible Options**: Choose between Schema Only, Data Only, or Schema + Data migration

### 🎯 **Migration Modes**
1. **Schema Only**: Migrate only table structures and database schema
2. **Data Only**: Migrate only data (requires existing schema in destination)
3. **Schema + Data**: Complete migration including both structure and data

### 📊 **User Interface**
- **Intuitive Layout**: Two-column design with schema selection and migration controls
- **Progress Tracking**: Real-time progress bars and operation status
- **Tree View**: Hierarchical display of schemas, tables, and estimated row counts
- **Migration Statistics**: Track schemas migrated, tables migrated, records processed

## How to Use

### Step 1: Access the PostgreSQL Tab
1. Launch the application: `python gui_launcher.py`
2. Click on the **"PostgreSQL Migration"** tab

### Step 2: Configure Connections
1. **Source Connection**: Enter your source PostgreSQL connection string
2. **Destination Connection**: Enter your destination PostgreSQL connection string
3. Click **"Connect to PostgreSQL"**

**Connection String Format:**
```
postgresql://username:password@server.postgres.database.azure.com:5432/database_name
```

**Example:**
```
postgresql://myuser:mypassword@myserver.postgres.database.azure.com:5432/mydatabase
```

### Step 3: Browse Schemas and Tables
1. Click **"Refresh Schemas"** to load available schemas
2. Select a schema from the dropdown to view its tables
3. The tree view will show:
   - Schema names with total record counts
   - Individual tables with estimated row counts

### Step 4: Choose Migration Type
Select your preferred migration approach:
- **Schema Only**: For setting up table structures
- **Data Only**: For migrating data to existing tables
- **Schema + Data**: For complete database migration

### Step 5: Execute Migration
Choose one of the migration options:

#### **Migrate Selected Schema**
- Select a schema from the dropdown
- Click **"Migrate Selected Schema"**
- Migrates all tables within the selected schema

#### **Migrate Selected Table**
- Select a specific table in the tree view
- Click **"Migrate Selected Table"**
- Migrates only the selected table

#### **Migrate All Schemas**
- Click **"Migrate All Schemas"**
- Migrates all available schemas (use with caution)

### Step 6: Monitor Progress
- Watch the **Migration Progress** section for real-time updates
- Monitor current operations and completion statistics
- View detailed logs in the **Logs** tab

## Technical Implementation

### Backend Components

#### **PostgreSQLConnectionManager**
- **File**: `src/postgresql_connection_manager.py`
- **Features**:
  - Connection pooling with configurable pool sizes
  - Exponential backoff retry logic
  - Health monitoring with automatic reconnection
  - Support for Azure managed identity authentication

#### **PostgreSQLMigrationService**
- **File**: `src/postgresql_migration_service.py`
- **Features**:
  - Schema extraction and DDL generation
  - Batch data migration with configurable batch sizes
  - Progress tracking and statistics
  - Error handling and retry mechanisms

#### **Enhanced Configuration**
- **File**: `src/config.py`
- **New Settings**:
  - `pg_source_connection_string`: Source PostgreSQL connection
  - `pg_dest_connection_string`: Destination PostgreSQL connection
  - Enhanced retry and health check settings

### GUI Components

#### **New Tab Interface**
- **Connection Section**: PostgreSQL connection strings and status
- **Schema Selection**: Dropdown and tree view for schema/table browsing
- **Migration Controls**: Radio buttons for migration type selection
- **Progress Tracking**: Progress bars and statistics display

#### **Background Processing**
- All database operations run in background threads
- Result queue system for UI updates
- Comprehensive error handling and user feedback

## Best Practices

### 🔒 **Security**
- Store connection strings securely (consider using environment variables)
- Use Azure managed identity when possible
- Implement least-privilege access for database users

### ⚡ **Performance**
- Use appropriate batch sizes for large data migrations
- Consider running migrations during off-peak hours
- Monitor source database performance during migration

### 🛡️ **Reliability**
- Test migrations with a small subset of data first
- Keep source databases as read-only during migration when possible
- Verify data integrity after migration completion

### 📝 **Planning**
- Document schema dependencies before migration
- Plan migration order for related schemas
- Have rollback procedures ready

## Environment Variables

You can configure the application using environment variables in a `.env` file:

```env
# PostgreSQL Connections
PG_SOURCE_CONNECTION_STRING=postgresql://user:pass@source.postgres.database.azure.com:5432/sourcedb
PG_DEST_CONNECTION_STRING=postgresql://user:pass@dest.postgres.database.azure.com:5432/destdb

# Performance Settings
BATCH_SIZE=1000
TIMEOUT_MS=30000

# Retry Configuration
CONNECTION_RETRY_ATTEMPTS=5
CONNECTION_RETRY_DELAY_MS=2000
OPERATION_RETRY_ATTEMPTS=3
HEALTH_CHECK_INTERVAL_SECONDS=30

# Authentication
USE_MANAGED_IDENTITY=false
```

## Troubleshooting

### Common Issues

#### **Connection Failures**
- Verify connection strings are correct
- Check firewall rules on Azure PostgreSQL
- Ensure proper authentication credentials

#### **Permission Errors**
- Verify user has necessary permissions on both databases
- Check if user can create schemas/tables in destination
- Ensure user has read access to source database

#### **Performance Issues**
- Adjust batch size in configuration
- Monitor connection pool settings
- Consider migration timing during low-usage periods

#### **Data Consistency**
- Verify foreign key constraints
- Check for data type compatibility
- Review character encoding settings

### Log Analysis
- Check the **Logs** tab for detailed operation information
- Look for retry attempts and connection health status
- Monitor migration statistics for progress validation

## Support and Maintenance

### Monitoring
- Use the built-in health monitoring for connection status
- Monitor PostgreSQL server metrics during migration
- Track migration progress through the GUI statistics

### Updates
- The PostgreSQL functionality follows Azure best practices
- Connection management includes automatic retry and recovery
- Schema migration handles complex database structures

## Azure Integration

### Azure Database for PostgreSQL
- **Fully Compatible**: Works with Azure Database for PostgreSQL - Single Server and Flexible Server
- **Authentication**: Supports both password and Azure AD authentication
- **Security**: Implements SSL connections and secure credential handling
- **Monitoring**: Integrates with Azure monitoring and logging capabilities

### Best Practices for Azure
- Use Azure Key Vault for connection string storage
- Implement Azure Private Link for secure connections
- Monitor costs using Azure Cost Management
- Use Azure Backup for pre-migration database backups

---

## Quick Start Example

1. **Launch Application**:
   ```bash
   python gui_launcher.py
   ```

2. **Navigate to PostgreSQL Tab**

3. **Enter Connections**:
   - Source: `postgresql://myuser:mypass@source.postgres.database.azure.com:5432/sourcedb`
   - Destination: `postgresql://myuser:mypass@dest.postgres.database.azure.com:5432/destdb`

4. **Connect and Refresh Schemas**

5. **Select Migration Type**: "Schema + Data"

6. **Choose Migration Scope**: "Migrate Selected Schema"

7. **Monitor Progress** in the Progress section

The PostgreSQL migration functionality is now fully integrated and ready for production use! 🚀
