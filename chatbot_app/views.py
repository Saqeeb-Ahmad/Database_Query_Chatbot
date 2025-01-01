from rest_framework.views import APIView
from rest_framework.response import Response
from django.db import connection
from typing import Tuple, List, Dict, Any
import logging
from .utils import (
    generate_sql_query, 
    validate_query, 
    get_table_names, 
    DatabaseError, 
    APIConfigError,
    QueryValidationError
)

logger = logging.getLogger(__name__)

class ChatbotView(APIView):
    def post(self, request) -> Response:
        """Handle POST requests with user queries."""
        user_input = request.data.get('input')
        
        if not user_input:
            return Response(
                {"error": "No input provided"}, 
                status=400
            )

        try:
            # Generate and validate SQL query
            sql_query = generate_sql_query(user_input)
            valid_tables = get_table_names()
            validate_query(sql_query, valid_tables)

            # Execute query and get results
            query_result = self._execute_query(sql_query)
            results = query_result[0]
            columns = query_result[1]

            
            # Generate summary
            summary = self._format_summary(results, columns)

            return Response({
                "status": "success",
                "summary": summary,
                "user_input": user_input,
                "sql_query": sql_query,
                "results": [dict(zip(columns, row)) for row in results]
            })

        except (DatabaseError, QueryValidationError) as e:
            logger.error(f"Database/Query Error: {str(e)}")
            return Response({
                "status": "error",
                "error": str(e),
                "user_input": user_input
            }, status=400)
            
        except APIConfigError as e:
            logger.error(f"API Configuration Error: {str(e)}")
            return Response({
                "status": "error",
                "error": "API configuration error. Please contact the administrator.",
                "details": str(e),
                "user_input": user_input
            }, status=500)

        except Exception as e:
            logger.error(f"Unexpected Error: {str(e)}", exc_info=True)
            return Response({
                "status": "error",
                "error": "An unexpected error occurred.",
                "details": str(e),
                "user_input": user_input
            }, status=500)

    def _execute_query(self, sql_query: str):
    """Execute SQL query and return results with column names."""
    # Initialize results and columns
    results = []
    columns = []

    # Open a database cursor and execute the query
    with connection.cursor() as cursor:
        cursor.execute(sql_query)

        # Fetch all the results from the query
        results = cursor.fetchall()

        # Extract column names from cursor description
        for col in cursor.description:
            columns.append(col[0])

    return results, columns

    def _format_summary(self, results, columns):
    """Format query results into a readable summary."""
    if not results:
        return "No results found."

    result_count = len(results)
    summary = [f"Found {result_count} result{'s' if result_count != 1 else ''}:"]

    for row in results:
        # Convert row to dictionary for easier access
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col] = row[i]

        # Initialize list for formatted line items
        line_items = []

        # Add product information if available
        if 'Name' in row_dict:
            line_items.append(str(row_dict['Name']))

        # Handle price information
        price_fields = ['ListPrice', 'StandardCost', 'UnitPrice']
        for price_field in price_fields:
            if price_field in row_dict and row_dict[price_field] is not None:
                line_items.append(f"${row_dict[price_field]:.2f}")
                break

        # Add other relevant fields
        skip_fields = ['Name', 'ListPrice', 'StandardCost', 'UnitPrice']
        for col, val in row_dict.items():
            if col not in skip_fields and val is not None:
                line_items.append(f"{col}: {val}")

        # Add formatted line items to the summary
        summary.append("• " + " - ".join(line_items))

    return "\n".join(summary)
