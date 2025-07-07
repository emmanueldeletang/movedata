"""
PostgreSQL migration service for Azure Database Migration Tool.
Implements schema and data migration between PostgreSQL databases.
"""

import logging
import time
import traceback
import psycopg2
from psycopg2 import sql, OperationalError, DatabaseError
from psycopg2.extras import execute_batch
from tqdm import tqdm

logger = logging.getLogger(__name__)

class PostgreSQLMigrationService:
    """Service to handle migration of schemas and data between PostgreSQL instances."""
    
    def __init__(self, source_pool, dest_pool, config, connection_manager=None, batch_size=1000):
        """Initialize the PostgreSQL migration service.
        
        Args:
            source_pool: PostgreSQL connection pool for source
            dest_pool: PostgreSQL connection pool for destination
            config: Configuration object
            connection_manager: PostgreSQL connection manager for retry functionality
            batch_size: Number of records to process in each batch
        """
        self.source_pool = source_pool
        self.dest_pool = dest_pool
        self.config = config
        self.connection_manager = connection_manager
        self.batch_size = batch_size
        
    def list_databases(self):
        """List all databases in the source PostgreSQL instance.
        
        Returns:
            list: List of database names
        """
        def _list_databases_operation():
            conn = self.source_pool.getconn()
            try:
                with conn.cursor() as cursor:
                    # Get databases excluding system databases
                    cursor.execute("""
                        SELECT datname 
                        FROM pg_database 
                        WHERE datistemplate = false 
                        AND datname NOT IN ('postgres', 'template0', 'template1', 'azure_maintenance')
                        ORDER BY datname
                    """)
                    databases = [row[0] for row in cursor.fetchall()]
                    return databases
            finally:
                self.source_pool.putconn(conn)
        
        try:
            if self.connection_manager:
                return self.connection_manager.execute_with_retry(_list_databases_operation, "source")
            else:
                return _list_databases_operation()
        except Exception as e:
            logger.error(f"Error listing PostgreSQL databases: {e}")
            return []
            
    def list_schemas(self, database_name=None):
        """List all schemas in the source database.
        
        Args:
            database_name: Name of the database (if None, uses current connection)
            
        Returns:
            list: List of schema names
        """
        def _list_schemas_operation():
            conn = self.source_pool.getconn()
            try:
                with conn.cursor() as cursor:
                    # Get schemas excluding system schemas
                    cursor.execute("""
                        SELECT schema_name 
                        FROM information_schema.schemata 
                        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
                        ORDER BY schema_name
                    """)
                    schemas = [row[0] for row in cursor.fetchall()]
                    return schemas
            finally:
                self.source_pool.putconn(conn)
        
        try:
            if self.connection_manager:
                return self.connection_manager.execute_with_retry(_list_schemas_operation, "source")
            else:
                return _list_schemas_operation()
        except Exception as e:
            logger.error(f"Error listing PostgreSQL schemas: {e}")
            return []
            
    def list_tables(self, schema_name='public'):
        """List all tables in a schema.
        
        Args:
            schema_name: Name of the schema
            
        Returns:
            list: List of table names with metadata
        """
        def _list_tables_operation():
            conn = self.source_pool.getconn()
            try:
                with conn.cursor() as cursor:
                    # Get tables with row counts
                    cursor.execute("""
                        SELECT 
                            t.table_name,
                            COALESCE(c.reltuples::bigint, 0) as estimated_rows
                        FROM information_schema.tables t
                        LEFT JOIN pg_class c ON c.relname = t.table_name
                        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
                        WHERE t.table_schema = %s 
                        AND t.table_type = 'BASE TABLE'
                        ORDER BY t.table_name
                    """, (schema_name,))
                    
                    tables = []
                    for row in cursor.fetchall():
                        tables.append({
                            'name': row[0],
                            'estimated_rows': row[1],
                            'schema': schema_name
                        })
                    return tables
            finally:
                self.source_pool.putconn(conn)
        
        try:
            if self.connection_manager:
                return self.connection_manager.execute_with_retry(_list_tables_operation, "source")
            else:
                return _list_tables_operation()
        except Exception as e:
            logger.error(f"Error listing PostgreSQL tables for schema {schema_name}: {e}")
            return []
            
    def get_table_count(self, schema_name, table_name):
        """Get the exact count of records in a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            int: Number of records in the table
        """
        def _count_operation():
            conn = self.source_pool.getconn()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name)
                        )
                    )
                    return cursor.fetchone()[0]
            finally:
                self.source_pool.putconn(conn)
        
        try:
            if self.connection_manager:
                return self.connection_manager.execute_with_retry(_count_operation, "source")
            else:
                return _count_operation()
        except Exception as e:
            logger.error(f"Error counting records in {schema_name}.{table_name}: {e}")
            return 0
            
    def migrate_schema(self, schema_name='public'):
        """Migrate schema structure from source to destination.
        
        Args:
            schema_name: Name of the schema to migrate
            
        Returns:
            dict: Migration statistics
        """
        try:
            logger.info(f"Starting schema migration for: {schema_name}")
            
            stats = {
                "success": True,
                "schema_name": schema_name,
                "tables_created": 0,
                "indexes_created": 0,
                "constraints_created": 0,
                "errors": []
            }
            
            # Step 1: Create schema if it doesn't exist
            self._create_schema_if_not_exists(schema_name)
            
            # Step 2: Get schema structure
            schema_ddl = self._extract_schema_ddl(schema_name)
            
            # Step 3: Create tables
            for table_ddl in schema_ddl['tables']:
                try:
                    self._execute_ddl(table_ddl)
                    stats["tables_created"] += 1
                    logger.info(f"Created table: {table_ddl['table_name']}")
                except Exception as e:
                    error_msg = f"Failed to create table {table_ddl['table_name']}: {e}"
                    logger.error(error_msg)
                    stats["errors"].append(error_msg)
            
            # Step 4: Create indexes
            for index_ddl in schema_ddl['indexes']:
                try:
                    self._execute_ddl(index_ddl)
                    stats["indexes_created"] += 1
                    logger.debug(f"Created index: {index_ddl['index_name']}")
                except Exception as e:
                    error_msg = f"Failed to create index {index_ddl['index_name']}: {e}"
                    logger.warning(error_msg)
                    stats["errors"].append(error_msg)
            
            # Step 5: Create constraints
            for constraint_ddl in schema_ddl['constraints']:
                try:
                    self._execute_ddl(constraint_ddl)
                    stats["constraints_created"] += 1
                    logger.debug(f"Created constraint: {constraint_ddl['constraint_name']}")
                except Exception as e:
                    error_msg = f"Failed to create constraint {constraint_ddl['constraint_name']}: {e}"
                    logger.warning(error_msg)
                    stats["errors"].append(error_msg)
            
            logger.info(f"Schema migration completed. Stats: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error migrating schema {schema_name}: {e}")
            logger.debug(traceback.format_exc())
            return {
                "success": False,
                "schema_name": schema_name,
                "error": str(e),
                "errors": [str(e)]
            }
            
    def migrate_table_data(self, schema_name, table_name):
        """Migrate data from a source table to destination table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            dict: Migration statistics
        """
        try:
            logger.info(f"Starting data migration for: {schema_name}.{table_name}")
            
            # Get table structure for column mapping
            columns = self._get_table_columns(schema_name, table_name)
            if not columns:
                raise Exception(f"Could not retrieve column information for {schema_name}.{table_name}")
            
            # Count total records
            total_records = self.get_table_count(schema_name, table_name)
            
            stats = {
                "success": True,
                "schema_name": schema_name,
                "table_name": table_name,
                "total_records": total_records,
                "migrated_records": 0,
                "failed_records": 0,
                "start_time": time.time()
            }
            
            if total_records == 0:
                logger.info(f"Table {schema_name}.{table_name} is empty, nothing to migrate")
                return stats
            
            # Create progress bar
            progress_bar = tqdm(total=total_records, desc=f"Migrating {schema_name}.{table_name}")
            
            # Process data in batches
            offset = 0
            while offset < total_records:
                try:
                    batch_stats = self._migrate_data_batch(schema_name, table_name, columns, offset)
                    stats["migrated_records"] += batch_stats["inserted"]
                    stats["failed_records"] += batch_stats["failed"]
                    progress_bar.update(batch_stats["processed"])
                    offset += self.batch_size
                    
                except Exception as e:
                    logger.error(f"Error migrating batch at offset {offset}: {e}")
                    stats["failed_records"] += self.batch_size
                    offset += self.batch_size
                    
            progress_bar.close()
            stats["end_time"] = time.time()
            stats["duration"] = stats["end_time"] - stats["start_time"]
            
            logger.info(f"Data migration completed for {schema_name}.{table_name}. "
                       f"Migrated: {stats['migrated_records']}, Failed: {stats['failed_records']}, "
                       f"Duration: {stats['duration']:.2f}s")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error migrating table data {schema_name}.{table_name}: {e}")
            logger.debug(traceback.format_exc())
            return {
                "success": False,
                "schema_name": schema_name,
                "table_name": table_name,
                "error": str(e)
            }
            
    def _create_schema_if_not_exists(self, schema_name):
        """Create schema in destination if it doesn't exist."""
        def _create_schema_operation():
            conn = self.dest_pool.getconn()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                            sql.Identifier(schema_name)
                        )
                    )
                conn.commit()
            finally:
                self.dest_pool.putconn(conn)
        
        if self.connection_manager:
            self.connection_manager.execute_with_retry(_create_schema_operation, "destination")
        else:
            _create_schema_operation()
            
    def _extract_schema_ddl(self, schema_name):
        """Extract DDL statements for schema objects."""
        def _extract_ddl_operation():
            conn = self.source_pool.getconn()
            try:
                with conn.cursor() as cursor:
                    schema_ddl = {
                        'tables': [],
                        'indexes': [],
                        'constraints': []
                    }
                    
                    # Get table DDL
                    cursor.execute("""
                        SELECT 
                            table_name,
                            column_name,
                            data_type,
                            character_maximum_length,
                            numeric_precision,
                            numeric_scale,
                            is_nullable,
                            column_default,
                            ordinal_position
                        FROM information_schema.columns 
                        WHERE table_schema = %s 
                        ORDER BY table_name, ordinal_position
                    """, (schema_name,))
                    
                    tables = {}
                    for row in cursor.fetchall():
                        table_name = row[0]
                        if table_name not in tables:
                            tables[table_name] = {
                                'table_name': table_name,
                                'columns': [],
                                'ddl': None
                            }
                        
                        # Build column definition
                        col_def = f"{row[1]} {row[2]}"
                        if row[3]:  # character_maximum_length
                            col_def += f"({row[3]})"
                        elif row[4] and row[5]:  # numeric_precision and scale
                            col_def += f"({row[4]},{row[5]})"
                        elif row[4]:  # numeric_precision only
                            col_def += f"({row[4]})"
                        
                        if row[6] == 'NO':  # is_nullable
                            col_def += " NOT NULL"
                        
                        if row[7]:  # column_default
                            col_def += f" DEFAULT {row[7]}"
                        
                        tables[table_name]['columns'].append(col_def)
                    
                    # Generate CREATE TABLE statements
                    for table_name, table_info in tables.items():
                        ddl = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\\n"
                        ddl += ",\\n".join(f"  {col}" for col in table_info['columns'])
                        ddl += "\\n)"
                        table_info['ddl'] = ddl
                        schema_ddl['tables'].append(table_info)
                    
                    # Get indexes (excluding primary key and unique constraints)
                    cursor.execute("""
                        SELECT 
                            i.indexname as index_name,
                            i.tablename as table_name,
                            i.indexdef as index_definition
                        FROM pg_indexes i
                        WHERE i.schemaname = %s
                        AND i.indexdef NOT LIKE '%%UNIQUE%%'
                        AND i.indexname NOT LIKE '%%_pkey'
                    """, (schema_name,))
                    
                    for row in cursor.fetchall():
                        schema_ddl['indexes'].append({
                            'index_name': row[0],
                            'table_name': row[1],
                            'ddl': row[2].replace(f'{schema_name}.', f'{schema_name}.')
                        })
                    
                    # Get constraints
                    cursor.execute("""
                        SELECT 
                            tc.constraint_name,
                            tc.table_name,
                            tc.constraint_type,
                            kcu.column_name,
                            ccu.table_name AS foreign_table_name,
                            ccu.column_name AS foreign_column_name
                        FROM information_schema.table_constraints tc
                        LEFT JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                          AND tc.table_schema = kcu.table_schema
                        LEFT JOIN information_schema.constraint_column_usage ccu
                          ON ccu.constraint_name = tc.constraint_name
                          AND ccu.table_schema = tc.table_schema
                        WHERE tc.table_schema = %s
                        AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE')
                        ORDER BY tc.table_name, tc.constraint_name
                    """, (schema_name,))
                    
                    constraints = {}
                    for row in cursor.fetchall():
                        constraint_name = row[0]
                        if constraint_name not in constraints:
                            constraints[constraint_name] = {
                                'constraint_name': constraint_name,
                                'table_name': row[1],
                                'constraint_type': row[2],
                                'columns': [],
                                'foreign_table': row[4],
                                'foreign_columns': []
                            }
                        
                        if row[3]:
                            constraints[constraint_name]['columns'].append(row[3])
                        if row[5]:
                            constraints[constraint_name]['foreign_columns'].append(row[5])
                    
                    # Generate constraint DDL
                    for constraint_name, constraint_info in constraints.items():
                        if constraint_info['constraint_type'] == 'PRIMARY KEY':
                            ddl = f"ALTER TABLE {schema_name}.{constraint_info['table_name']} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({', '.join(constraint_info['columns'])})"
                        elif constraint_info['constraint_type'] == 'UNIQUE':
                            ddl = f"ALTER TABLE {schema_name}.{constraint_info['table_name']} ADD CONSTRAINT {constraint_name} UNIQUE ({', '.join(constraint_info['columns'])})"
                        elif constraint_info['constraint_type'] == 'FOREIGN KEY':
                            ddl = f"ALTER TABLE {schema_name}.{constraint_info['table_name']} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({', '.join(constraint_info['columns'])}) REFERENCES {schema_name}.{constraint_info['foreign_table']} ({', '.join(constraint_info['foreign_columns'])})"
                        else:
                            continue
                        
                        constraint_info['ddl'] = ddl
                        schema_ddl['constraints'].append(constraint_info)
                    
                    return schema_ddl
            finally:
                self.source_pool.putconn(conn)
        
        if self.connection_manager:
            return self.connection_manager.execute_with_retry(_extract_ddl_operation, "source")
        else:
            return _extract_ddl_operation()
            
    def _execute_ddl(self, ddl_info):
        """Execute a DDL statement on the destination."""
        def _execute_ddl_operation():
            conn = self.dest_pool.getconn()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(ddl_info['ddl'])
                conn.commit()
            finally:
                self.dest_pool.putconn(conn)
        
        if self.connection_manager:
            self.connection_manager.execute_with_retry(_execute_ddl_operation, "destination")
        else:
            _execute_ddl_operation()
            
    def _get_table_columns(self, schema_name, table_name):
        """Get column names for a table."""
        def _get_columns_operation():
            conn = self.source_pool.getconn()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = %s AND table_name = %s 
                        ORDER BY ordinal_position
                    """, (schema_name, table_name))
                    return [row[0] for row in cursor.fetchall()]
            finally:
                self.source_pool.putconn(conn)
        
        if self.connection_manager:
            return self.connection_manager.execute_with_retry(_get_columns_operation, "source")
        else:
            return _get_columns_operation()
            
    def _migrate_data_batch(self, schema_name, table_name, columns, offset):
        """Migrate a batch of data."""
        def _migrate_batch_operation():
            # Get data from source
            source_conn = self.source_pool.getconn()
            dest_conn = self.dest_pool.getconn()
            
            try:
                with source_conn.cursor() as source_cursor:
                    # Fetch batch data
                    source_cursor.execute(
                        sql.SQL("SELECT {} FROM {}.{} ORDER BY {} LIMIT %s OFFSET %s").format(
                            sql.SQL(', ').join(map(sql.Identifier, columns)),
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                            sql.Identifier(columns[0])  # Order by first column for consistency
                        ),
                        (self.batch_size, offset)
                    )
                    batch_data = source_cursor.fetchall()
                
                if not batch_data:
                    return {"inserted": 0, "failed": 0, "processed": 0}
                
                # Insert into destination
                with dest_conn.cursor() as dest_cursor:
                    insert_sql = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(table_name),
                        sql.SQL(', ').join(map(sql.Identifier, columns)),
                        sql.SQL(', ').join(sql.Placeholder() * len(columns))
                    )
                    
                    execute_batch(dest_cursor, insert_sql, batch_data, page_size=100)
                    dest_conn.commit()
                
                return {
                    "inserted": len(batch_data),
                    "failed": 0,
                    "processed": len(batch_data)
                }
                
            except Exception as e:
                logger.error(f"Error in batch migration: {e}")
                try:
                    dest_conn.rollback()
                except:
                    pass
                return {
                    "inserted": 0,
                    "failed": len(batch_data) if 'batch_data' in locals() else self.batch_size,
                    "processed": len(batch_data) if 'batch_data' in locals() else self.batch_size
                }
            finally:
                self.source_pool.putconn(source_conn)
                self.dest_pool.putconn(dest_conn)
        
        if self.connection_manager:
            return self.connection_manager.execute_with_retry(_migrate_batch_operation, "both")
        else:
            return _migrate_batch_operation()
