import google.generativeai as genai
from django.conf import settings
from django.db import connection
import json
import re

# Configure the Gemini API
genai.configure(api_key=settings.GEMINI_API_KEY)

def generate_sql_query(user_input):
    model = genai.GenerativeModel('gemini-pro')
    prompt = f"""
    Convert the following user input into a valid SQL query for the AdventureWorks database:
    User input: '{user_input}'
    Rules:
    1. Use only standard SQL syntax compatible with MySQL.
    2. The AdventureWorks database has the following main tables:
       - Product (ProductID, Name, ProductNumber, Color, StandardCost, ListPrice, Size, Weight, ProductCategoryID, ProductModelID)
       - ProductCategory (ProductCategoryID, Name)
       - SalesOrderHeader (SalesOrderID, OrderDate, DueDate, ShipDate, Status, CustomerID, TotalDue)
       - SalesOrderDetail (SalesOrderID, SalesOrderDetailID, OrderQty, ProductID, UnitPrice, LineTotal)
       - Customer (CustomerID, PersonID, StoreID, TerritoryID)
    3. For date comparisons, use 'CURDATE()' for the current date.
    4. Return only the SQL query, without any explanations or code block markers.
    5. Limit results to 10 rows unless specifically asked for more.
    """
    response = model.generate_content(prompt)
    query = response.text.strip()
    
    # Remove SQL code block markers if present
    query = re.sub(r'```sql|```', '', query)
    
    # Remove any leading/trailing whitespace and ensure it ends with a semicolon
    query = query.strip()
    if not query.endswith(';'):
        query += ';'
    
    return query

def execute_query(query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        if query.lower().strip().startswith('select'):
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        else:
            return {"affected_rows": cursor.rowcount}

def summarize_results(results):
    model = genai.GenerativeModel('gemini-pro')
    prompt = f"""
    Summarize the following database results in a user-friendly way:
    {json.dumps(results, indent=2)}
    Rules:
    1. Provide a concise summary.
    2. Include key information and any relevant statistics.
    3. Use natural language in your response.
    4. If the result is empty, mention that no data was found.
    """
    response = model.generate_content(prompt)
    return response.text.strip()

def process_user_input(user_input):
    try:
        sql_query = generate_sql_query(user_input)
        results = execute_query(sql_query)
        summary = summarize_results(results)
        return {
            "user_input": user_input,
            "generated_query": sql_query,
            "query_results": results,
            "summary": summary
        }
    except Exception as e:
        return {"error": str(e)}
    
