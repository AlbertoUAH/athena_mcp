#!/usr/bin/env python3
import boto3
import time
from fastmcp import FastMCP

DATABASE_NAME = "drm_text2sql_db"
OUTPUT_LOCATION = "s3://glab-drm-query-results/"

mcp = FastMCP("athena-server")

_athena_client = None

def get_athena_client():
    global _athena_client
    if _athena_client is None:
        _athena_client = boto3.client('athena', region_name='eu-west-1')
    return _athena_client

def _execute_query_internal(query: str, output_location: str = OUTPUT_LOCATION) -> str:
    """Execute a SQL query on AWS Athena in the drm_text2sql_db database
    
    Args:
        query: The SQL query to execute
        output_location: S3 location for query results (optional)
    """
    try:
        client = get_athena_client()
        
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': DATABASE_NAME},
            ResultConfiguration={'OutputLocation': output_location}
        )
        
        query_execution_id = response['QueryExecutionId']
        
        max_attempts = 30
        attempt = 0
        while attempt < max_attempts:
            query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_status['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            
            time.sleep(1)
            attempt += 1
        
        if status == 'SUCCEEDED':
            results = client.get_query_results(QueryExecutionId=query_execution_id)
            
            rows = results['ResultSet']['Rows']
            if not rows:
                return "Query executed successfully but returned no results."
            
            headers = [col['VarCharValue'] for col in rows[0]['Data']]
            
            response_text = f"Query executed successfully!\n\n"
            response_text += f"Columns: {', '.join(headers)}\n\n"
            response_text += f"Results ({len(rows)-1} rows):\n"
            
            for row in rows[1:11]:
                values = [col.get('VarCharValue', 'NULL') for col in row['Data']]
                response_text += f"{' | '.join(values)}\n"
            
            if len(rows) > 11:
                response_text += f"\n... and {len(rows)-11} more rows"
            
            return response_text
        
        elif status == 'FAILED':
            error = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            return f"Query failed: {error}"
        
        elif status == 'CANCELLED':
            return "Query was cancelled."
        
        else:
            return f"Query timed out after {max_attempts} seconds. Status: {status}"
        
    except Exception as e:
        import traceback
        return f"Error: {str(e)}\n{traceback.format_exc()}"

@mcp.tool()
def execute_athena_query(query: str, output_location: str = OUTPUT_LOCATION) -> str:
    """Execute a SQL query on AWS Athena in the drm_text2sql_db database
    
    Args:
        query: The SQL query to execute
        output_location: S3 location for query results (optional)
    """
    return _execute_query_internal(query, output_location)

@mcp.tool()
def list_tables() -> str:
    """List all tables in the drm_text2sql_db database"""
    try:
        query = f"SHOW TABLES IN {DATABASE_NAME}"
        return _execute_query_internal(query)
    except Exception as e:
        import traceback
        return f"Error: {str(e)}\n{traceback.format_exc()}"

@mcp.tool()
def describe_table(table_name: str) -> str:
    """Describe the schema of a specific table in drm_text2sql_db
    
    Args:
        table_name: Name of the table to describe
    """
    try:
        query = f"DESCRIBE {DATABASE_NAME}.{table_name}"
        return _execute_query_internal(query)
    except Exception as e:
        import traceback
        return f"Error: {str(e)}\n{traceback.format_exc()}"

@mcp.tool()
def get_table_sample(table_name: str, limit: int = 10) -> str:
    """Get a sample of rows from a specific table
    
    Args:
        table_name: Name of the table
        limit: Number of rows to return (default: 10)
    """
    try:
        query = f"SELECT * FROM {DATABASE_NAME}.{table_name} LIMIT {limit}"
        return _execute_query_internal(query)
    except Exception as e:
        import traceback
        return f"Error: {str(e)}\n{traceback.format_exc()}"
