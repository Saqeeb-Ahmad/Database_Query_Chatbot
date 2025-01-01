import google.generativeai as genai
from django.db import connection
from django.conf import settings
from typing import List, Dict
import re
import logging

logger = logging.getLogger(__name__)

# Custom exceptions
class DatabaseError(Exception):
    """Custom exception for database-related errors."""
    pass

class APIConfigError(Exception):
    """Custom exception for API configuration errors."""
    pass

class QueryValidationError(Exception):
    """Custom exception for query validation errors."""
    pass

# Table mapping for common terms
TABLE_MAPPINGS = {
    'product': 'production_product',
    'products': 'production_product',
    'category': 'production_productcategory',
    'categories': 'production_productcategory',
    'customer': 'sales_customer',
    'customers': 'sales_customer',
    'order': 'sales_salesorderheader',
    'orders': 'sales_salesorderheader',
    'order_details': 'sales_salesorderdetail',
    'employee': 'humanresources_employee',
    'employees': 'humanresources_employee',
    'department': 'humanresources_department',
    'departments': 'humanresources_department',
    'person': 'person_person',
    'address': 'person_address',
}

def initialize_gemini_api():
    """Initialize Gemini API with proper error handling."""
    try:
        api_key = settings.GEMINI_API_KEY
        if not api_key:
            raise APIConfigError("GEMINI_API_KEY not found in settings")
        genai.configure(api_key=api_key)
        return True
    except Exception as e:
        logger.error(f"Failed to initialize Gemini API: {str(e)}")
        raise APIConfigError(f"Failed to initialize Gemini API: {str(e)}")

def get_table_names():
    """Get list of valid table names from database."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            # Initialize an empty list to store table names
            table_names = []
            for table in tables:
                table_names.append(table[0])

            return table_names
    except Exception as e:
        logger.error(f"Database error while fetching tables: {str(e)}")
        raise DatabaseError(f"Failed to fetch table names: {str(e)}")


def get_table_info():
    """Get table information including columns for each table."""
    table_info = {}
    try:
        with connection.cursor() as cursor:
            # Get table names first
            table_names = get_table_names()

            # Iterate over each table to fetch columns
            for table in table_names:
                cursor.execute(f"DESCRIBE {table}")
                columns = []
                for row in cursor.fetchall():
                    columns.append(f"{row[0]} ({row[1]})")
                table_info[table] = columns

        return table_info
    except Exception as e:
        logger.error(f"Error getting table info: {str(e)}")
        raise DatabaseError(f"Failed to get table info: {str(e)}")


def validate_query(query, valid_tables):
    """Validate the generated SQL query for security and correctness."""
    try:
        # Check if query is empty or None
        if not query or not query.strip():
            raise QueryValidationError("Empty query is not allowed")
        
        query_lower = query.lower()
        
        # Forbidden keywords to prevent unsafe operations
        forbidden_keywords = ['drop', 'truncate', 'delete', 'update', 'insert', 'alter', 'create', 'grant', 'revoke']
        if any(keyword in query_lower for keyword in forbidden_keywords):
            raise QueryValidationError("Query contains forbidden operation")

        # Only allow SELECT queries
        if not query_lower.startswith('select'):
            raise QueryValidationError("Only SELECT queries are allowed")

        # Extract table names in query
        tables_in_query = set(re.findall(r'(?:from|join)\s+(\w+)', query_lower))
        if not tables_in_query:
            raise QueryValidationError("No valid tables found in query")

        # Check if the tables exist in valid tables list
        invalid_tables = tables_in_query - {table.lower() for table in valid_tables}
        if invalid_tables:
            suggestions = [f"Instead of '{table}', did you mean: {', '.join([valid for valid in valid_tables if table in valid.lower()])}?" for table in invalid_tables]
            raise QueryValidationError(f"Invalid tables in query: {', '.join(invalid_tables)}. " + "\n".join(suggestions))

        # Check for basic query structure
        if not re.search(r'from\s+\w+', query_lower):
            raise QueryValidationError("Invalid query structure: Missing FROM clause")

        # Disallow comments and multiple statements
        if '--' in query or '/*' in query or ';' in query[:-1]:
            raise QueryValidationError("Comments and multiple statements are not allowed")
        
        return True
    except QueryValidationError:
        raise
    except Exception as e:
        logger.error(f"Query validation error: {str(e)}")
        raise QueryValidationError(str(e))


def generate_sql_query(user_input: str):
    """Generate SQL query from user input using Gemini API."""
    try:
        initialize_gemini_api()
        
        model = genai.GenerativeModel('gemini-pro')

        # Table relationships and common patterns
        table_relationships = """
        Important Table Relationships:
        - production_product joins with production_productcategory using ProductCategoryID
        - production_product joins with production_productsubcategory using ProductSubcategoryID
        """
        
        # Get schema information from database
        schema_info = []
        with connection.cursor() as cursor:
            for table in ["production_product", "production_productcategory", "production_productsubcategory"]:
                cursor.execute(f"SHOW COLUMNS FROM {table}")
                columns = [f"{table}.{row[0]} ({row[1]})" for row in cursor.fetchall()]
                schema_info.append(f"Table {table}:\n  " + "\n  ".join(columns))
        
        # Construct prompt for Gemini API
        prompt = f"""
        Convert this user request into a MySQL query:
        "{user_input}"

        Database Schema:
        {chr(10).join(schema_info)}

        {table_relationships}

        Rules:
        - Use only standard MySQL syntax
        - Always use table aliases (e.g., p for production_product)
        - Include LIMIT 100 unless a specific limit is requested
        """
        
        # Generate and clean query from API response
        response = model.generate_content(prompt)
        query = response.text.strip()
        query = ' '.join(query.split()).rstrip(';') + ';'
        
        return query
        
    except APIConfigError as e:
        raise APIConfigError(str(e))
    except Exception as e:
        logger.error(f"Error generating SQL query: {str(e)}")
        raise DatabaseError(f"Failed to generate SQL query: {str(e)}")


def format_result_summary(results, columns):
    """Format query results into a readable summary."""
    if not results:
        return "No products found."

    summary = [f"Found {len(results)} product{'s' if len(results) != 1 else ''}:"]

    for result in results:
        result_dict = dict(zip(columns, result))

        # Format product info
        name = result_dict.get('Name', result_dict.get('ProductName', ''))
        price = result_dict.get('ListPrice', result_dict.get('StandardCost', None))
        price_str = f"${price:.2f}" if price is not None else ""
        
        line_items = [name, price_str] if name or price_str else []
        
        # Add other fields
        for col, val in result_dict.items():
            if col not in ['Name', 'ProductName', 'ListPrice', 'StandardCost'] and val is not None:
                line_items.append(f"{col}: {val}")

        summary.append(f"• {' - '.join(line_items)}")
    
    return "\n".join(summary)


def format_currency(amount):
    """Format amount as currency."""
    try:
        return f"${amount:,.2f}"
    except (TypeError, ValueError):
        return str(amount)

def clean_column_name(name):
    """Clean and format column name for display."""
    return name.replace('_', ' ').title()

