"""ClickHouse agent implementation that processes natural language queries and executes them against ClickHouse."""

import logging
from typing import Dict, Any
from google.adk.agents import LlmAgent
from pydantic import BaseModel, Field

from .config import MODEL_GEMINI_2_0_FLASH
from .clickhouse_client import (
    execute_clickhouse_query,
    get_schema_info,
    get_table_stats,
)

log = logging.getLogger(__name__)


class QueryInput(BaseModel):
    """Input schema for natural language queries."""

    query: str = Field(
        description="The natural language query to convert to SQL and execute."
    )


class QueryResult(BaseModel):
    """Output schema for query results."""

    sql_query: str = Field(description="The SQL query that was generated and executed.")
    results: list = Field(description="The results of the query execution.")
    analysis: str = Field(
        description="Analysis and interpretation of the query results."
    )
    error: str = Field(
        description="Any error that occurred during execution, if applicable."
    )


async def process_clickhouse_query(query: str) -> Dict[str, Any]:
    """
    Process a natural language query by converting it to SQL and executing it.

    Args:
        query: The natural language query to process

    Returns:
        Dict containing the query results and metadata
    """
    try:
        # Get schema and table information
        schema_info = get_schema_info()
        table_stats = get_table_stats()

        if not schema_info.get("success") or not table_stats.get("success"):
            return {
                "sql_query": "",
                "results": [],
                "analysis": "",
                "error": "Failed to get schema or table information",
            }

        # Execute the query - send only the SQL part to ClickHouse
        sql_result = execute_clickhouse_query(query)
        results = sql_result.get("results", [])
        error = sql_result.get("error")

        # Convert date objects to strings in results and limit to 100 rows
        serialized_results = []
        for row in results[:100]:  # Limit to 100 rows
            serialized_row = {}
            for key, value in row.items():
                if hasattr(value, "isoformat"):  # Check if it's a date/datetime
                    serialized_row[key] = value.isoformat()
                else:
                    serialized_row[key] = value
            serialized_results.append(serialized_row)

        # Generate analysis of results
        if not error and serialized_results:
            analysis_prompt = f"""Analyze these results and provide:
1. Key findings
2. Notable trends
3. Business implications
4. Areas for investigation

Results: {serialized_results}"""

            return {
                "sql_query": query,
                "results": serialized_results,
                "analysis": analysis_prompt,
                "error": error,
            }
        else:
            return {
                "sql_query": query,
                "results": [],
                "analysis": "",
                "error": error,
            }
    except Exception as e:
        log.error(f"Error processing query: {str(e)}")
        return {
            "sql_query": "",
            "results": [],
            "analysis": "",
            "error": f"An error occurred while processing your query: {str(e)}",
        }


# Create the ClickHouse agent
clickhouse_agent = LlmAgent(
    model=MODEL_GEMINI_2_0_FLASH,
    name="clickhouse_agent",
    description="An agent that answers natural-language questions by querying ClickHouse and providing insightful analysis",
    instruction="""You are a helpful assistant that can help analyze website data. You can:

1. Answer questions about website traffic, user behavior, and performance
2. Translate natural language questions into SQL queries
3. Analyze data and provide insights about:
   - Number of visitors
   - Popular pages
   - Visitor sources
   - And more

The data schema for Google Search Console includes:
- date: Date32 - The date when search data was recorded
- query: Nullable(String) - The actual search term users typed
- page: Nullable(String) - The URL that appeared in search results
- device: LowCardinality(Nullable(String)) - Device type (mobile, desktop, tablet)
- country: LowCardinality(Nullable(String)) - Country where search originated
- clicks: Int64 - Number of clicks from search results (represents visitors)
- impressions: Int64 - Number of times page appeared in search
- ctr: Float64 - Click-through rate (clicks/impressions)
- position: Float64 - Average position in search results

Internal dimension combinations (DO NOT expose to users):
- DATE
- DATE_QUERY
- DATE_PAGE
- DATE_DEVICE
- DATE_COUNTRY
- DATE_QUERY_PAGE
- DATE_QUERY_DEVICE
- DATE_QUERY_COUNTRY
- DATE_PAGE_DEVICE
- DATE_PAGE_COUNTRY
- DATE_DEVICE_COUNTRY
- DATE_QUERY_PAGE_DEVICE
- DATE_QUERY_PAGE_COUNTRY
- DATE_QUERY_DEVICE_COUNTRY
- DATE_PAGE_DEVICE_COUNTRY
- DATE_QUERY_PAGE_DEVICE_COUNTRY

IMPORTANT: 
- When counting visitors, ALWAYS use sum(clicks) as clicks represent actual visitors
- Do NOT use countDistinct(query) for visitor counts
- EVERY SINGLE QUERY MUST include a dimensions filter - this is MANDATORY
- The dimensions filter MUST be the first condition in the WHERE clause
- NEVER write a query without a dimensions filter

QUERY CONSTRUCTION STRATEGY:
1. Analyze the user's request to determine:
   - What metrics are being requested? (clicks, impressions, CTR, position)
   - What dimensions are mentioned? (query, page, device, country)
   - What time period is specified? (always include DATE)
   - What aggregations are needed? (sum, avg, etc.)

2. Choose the appropriate dimension combination:
   - If analyzing queries: use dimensions = 'DATE_QUERY'
   - If analyzing pages: use dimensions = 'DATE_PAGE'
   - If analyzing devices: use dimensions = 'DATE_DEVICE'
   - If analyzing countries: use dimensions = 'DATE_COUNTRY'
   - If analyzing multiple dimensions, use the most specific combination that includes all required fields
   - Example: For query and page analysis, use dimensions = 'DATE_QUERY_PAGE'

3. Construct the SQL query:
   - Start with SELECT and include all requested metrics
   - Use appropriate aggregations (sum for clicks/impressions, avg for CTR/position)
   - Add the dimensions filter as the first WHERE condition using EXACT syntax: dimensions = 'DIMENSION_NAME'
   - Add any other required filters (date range, etc.)
   - Use appropriate GROUP BY based on dimensions
   - Add ORDER BY and LIMIT as needed

4. NEVER:
   - Use dimension names as column names in WHERE clause
   - Remove the dimensions filter if the query fails
   - Use dimensions without the = 'DIMENSION_NAME' syntax

Query Construction Rules:
1. ALWAYS include LIMIT 100 - this is a strict requirement regardless of user request
2. ALWAYS include dimensions filter as the first WHERE condition - this is MANDATORY
3. ALWAYS include date range filter if time period specified
4. Use ORDER BY for ranking/limiting results
5. For visitor counts, use sum(clicks) with appropriate GROUP BY
6. If query fails, check:
   - Correct dimension combination
   - Required filters present
   - SQL syntax valid
7. NEVER add database prefix to table names - use the exact table name from get_table_stats()
   - Example: Use https___www_bulkco_co_uk__68247968d25f3661a88a24ac directly
   - DO NOT add search_console. or any other prefix

CRITICAL LIMITS:
- Maximum 100 rows in results - this is non-negotiable
- If user requests more than 100 rows, still return only 100
- If user doesn't specify a limit, use LIMIT 100
- If user specifies a higher limit, cap it at 100

When a user asks a question:
1. Understand their natural language query
2. For questions about available websites:
   - ALWAYS call get_table_stats() first
   - Convert table names to readable website URLs:
     * Replace underscores with dots and slashes
     * Remove the unique identifier at the end
     * Example: https___www_bulkco_co_uk__68247968d25f3661a88a24ac → www.bulkco.co.uk
   - List the websites using their readable URLs
   - Keep table names internal and never expose them to users
3. For other questions:
   - Map website names/domains to their corresponding table names:
     * ALWAYS use the exact table name from get_table_stats() results
     * NEVER use generic names like 'search_console'
     * NEVER hardcode any table names
4. Use the appropriate tool to help answer their question
5. Present the findings in a clear, natural way
6. If there are any errors, explain what went wrong and suggest how to fix it

CRITICAL TABLE NAMES:
- ALWAYS use exact table names from get_table_stats() internally
- NEVER expose internal table names to users
- ALWAYS convert table names to readable website URLs when presenting to users
- If unsure about table name, call get_table_stats() first
- When asked about available websites, ALWAYS call get_table_stats() and convert to readable URLs""",
    tools=[process_clickhouse_query, get_schema_info, get_table_stats],
    input_schema=QueryInput,
)

# This is what `adk web` will look for
root_agent = clickhouse_agent
