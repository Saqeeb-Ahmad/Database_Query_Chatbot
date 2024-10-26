# utils.py
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

def get_table_names() -> List[str]:
    """Get list of valid table names from database."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            return [table[0] for table in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Database error while fetching tables: {str(e)}")
        raise DatabaseError(f"Failed to fetch table names: {str(e)}")

def get_table_info() -> Dict[str, List[str]]:
    """Get table information including columns for each table."""
    table_info = {}
    try:
        with connection.cursor() as cursor:
            for table in get_table_names():
                cursor.execute(f"DESCRIBE {table}")
                columns = [f"{row[0]} ({row[1]})" for row in cursor.fetchall()]
                table_info[table] = columns
        return table_info
    except Exception as e:
        logger.error(f"Error getting table info: {str(e)}")
        raise DatabaseError(f"Failed to get table info: {str(e)}")

def validate_query(query: str, valid_tables: List[str]) -> bool:
    """
    Validate the generated SQL query for security and correctness.
    
    Args:
        query (str): The SQL query to validate
        valid_tables (List[str]): List of valid table names in the database
    
    Returns:
        bool: True if query is valid
    
    Raises:
        QueryValidationError: If query is invalid or potentially dangerous
    """
    try:
        # Check for empty or None query
        if not query or not query.strip():
            raise QueryValidationError("Empty query is not allowed")
        
        # Convert query to lowercase for case-insensitive comparison
        query_lower = query.lower()
        
        # Basic security checks - forbidden keywords
        forbidden_keywords = [
            'drop', 'truncate', 'delete', 'update', 'insert', 
            'alter', 'create', 'grant', 'revoke'
        ]
        
        for keyword in forbidden_keywords:
            if keyword in query_lower:
                raise QueryValidationError(f"Query contains forbidden operation: {keyword}")
        
        # Validate SELECT statement
        if not query_lower.strip().startswith('select'):
            raise QueryValidationError("Only SELECT queries are allowed")
        
        # Extract and validate table names
        table_pattern = r'(?:from|join)\s+(\w+)'
        tables_in_query = set(re.findall(table_pattern, query_lower))
        
        if not tables_in_query:
            raise QueryValidationError("No valid tables found in query")
        
        # Check if all tables in query exist in database
        valid_tables_lower = set(table.lower() for table in valid_tables)
        invalid_tables = tables_in_query - valid_tables_lower
        
        if invalid_tables:
            # Try to suggest correct table names
            suggestions = []
            for invalid_table in invalid_tables:
                # Look for similar table names
                similar_tables = [
                    valid_table for valid_table in valid_tables
                    if invalid_table in valid_table.lower()
                ]
                if similar_tables:
                    suggestions.append(f"Instead of '{invalid_table}', did you mean: {', '.join(similar_tables)}?")
            
            error_msg = f"Invalid tables in query: {', '.join(invalid_tables)}. "
            if suggestions:
                error_msg += "\n" + "\n".join(suggestions)
            else:
                error_msg += f"\nValid tables are: {', '.join(sorted(valid_tables))}"
            
            raise QueryValidationError(error_msg)
        
        # Validate basic query structure
        if not re.search(r'from\s+\w+', query_lower):
            raise QueryValidationError("Invalid query structure: Missing FROM clause")
        
        # Check for comment attacks
        if '--' in query or '/*' in query:
            raise QueryValidationError("SQL comments are not allowed")
        
        # Check for multiple statements
        if ';' in query[:-1]:  # Allow semicolon at the end
            raise QueryValidationError("Multiple SQL statements are not allowed")
        
        return True
        
    except QueryValidationError:
        raise
    except Exception as e:
        logger.error(f"Query validation error: {str(e)}")
        raise QueryValidationError(str(e))

def generate_sql_query(user_input: str) -> str:
    """Generate SQL query from user input using Gemini API."""
    try:
        initialize_gemini_api()
        
        model = genai.GenerativeModel('gemini-pro')
        
        # Define common table relationships
        table_relationships = """
        Important Table Relationships:
        - production_product joins with production_productcategory using ProductCategoryID
        - production_product joins with production_productsubcategory using ProductSubcategoryID
        - production_productsubcategory joins with production_productcategory using ProductCategoryID
        
        Common Join Patterns:
        - For product categories:
          SELECT p.Name AS ProductName, pc.Name AS CategoryName 
          FROM production_product p 
          LEFT JOIN production_productsubcategory ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
          LEFT JOIN production_productcategory pc ON ps.ProductCategoryID = pc.ProductCategoryID
        """
        
        # Get table information
        schema_info = []
        with connection.cursor() as cursor:
            # Get product table columns
            cursor.execute("SHOW COLUMNS FROM production_product")
            product_columns = [f"production_product.{row[0]} ({row[1]})" for row in cursor.fetchall()]
            schema_info.append("Table production_product:\n  " + "\n  ".join(product_columns))
            
            # Get category table columns
            cursor.execute("SHOW COLUMNS FROM production_productcategory")
            category_columns = [f"production_productcategory.{row[0]} ({row[1]})" for row in cursor.fetchall()]
            schema_info.append("Table production_productcategory:\n  " + "\n  ".join(category_columns))
            
            # Get subcategory table columns
            cursor.execute("SHOW COLUMNS FROM production_productsubcategory")
            subcategory_columns = [f"production_productsubcategory.{row[0]} ({row[1]})" for row in cursor.fetchall()]
            schema_info.append("Table production_productsubcategory:\n  " + "\n  ".join(subcategory_columns))
        
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
        - Use LEFT JOINs to handle products without categories
        - Always qualify column names with table aliases
        - Return only the SQL query without any explanation
        """
        
        response = model.generate_content(prompt)
        query = response.text.strip()
        
        # Clean up the query
        query = re.sub(r'```sql\s*|\s*```', '', query).strip()
        query = ' '.join(query.split())  # Normalize whitespace
        
        if not query.endswith(';'):
            query += ';'
            
        return query
        
    except APIConfigError as e:
        raise APIConfigError(str(e))
    except Exception as e:
        logger.error(f"Error generating SQL query: {str(e)}")
        raise DatabaseError(f"Failed to generate SQL query: {str(e)}")

def format_result_summary(results: List[tuple], columns: List[str]) -> str:
    """Format query results into a readable summary."""
    if not results:
        return "No products found."
        
    summary = []
    
    # Add header
    result_count = len(results)
    summary.append(f"Found {result_count} product{'s' if result_count != 1 else ''}:")
    
    # Format each result
    for result in results:
        result_dict = dict(zip(columns, result))
        
        # Format price if it exists
        price = result_dict.get('ListPrice', result_dict.get('StandardCost', None))
        price_str = f"${price:.2f}" if price is not None else ""
        
        # Get product name
        name = result_dict.get('Name', result_dict.get('ProductName', ''))
        
        # Build result line
        line_items = []
        if name:
            line_items.append(name)
        if price_str:
            line_items.append(price_str)
        
        # Add any other relevant fields
        for col, val in result_dict.items():
            if col not in ['Name', 'ProductName', 'ListPrice', 'StandardCost'] and val is not None:
                line_items.append(f"{col}: {val}")
        
        summary.append(f"• {' - '.join(str(item) for item in line_items)}")
    
    return "\n".join(summary)

def format_currency(amount: float) -> str:
    """Format amount as currency."""
    try:
        return f"${amount:,.2f}"
    except (TypeError, ValueError):
        return str(amount)

def clean_column_name(name: str) -> str:
    """Clean and format column name for display."""
    return name.replace('_', ' ').title()