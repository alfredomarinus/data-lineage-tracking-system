from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from typing import List, Dict, Any
import json
import logging
import os

from .parser import SQLParser
from .models import SQLRequest, LineageGraph, QueryAnalysis
from .lineage import LineageBuilder

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="SQL Lineage Tracker",
    description="A tool to track SQL data dependencies and visualize table relationships",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files if directory exists
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# Initialize components
sql_parser = SQLParser()
lineage_builder = LineageBuilder()

# Store query analyses for building lineage
query_analyses: List[QueryAnalysis] = []

@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Serve the main HTML page"""
    try:
        if os.path.exists("static/index.html"):
            with open("static/index.html", "r") as f:
                return HTMLResponse(content=f.read())
        else:
            return HTMLResponse(content="""
            <html>
                <head><title>SQL Lineage Tracker</title></head>
                <body>
                    <h1>SQL Lineage Tracker API</h1>
                    <p>API is running. Use the following endpoints:</p>
                    <ul>
                        <li>POST /api/parse - Parse SQL query</li>
                        <li>GET /api/lineage - Get lineage graph</li>
                        <li>GET /api/analyses - Get all query analyses</li>
                        <li>DELETE /api/reset - Reset all data</li>
                        <li>GET /health - Health check</li>
                    </ul>
                </body>
            </html>
            """)
    except Exception as e:
        logger.error(f"Error serving root page: {str(e)}")
        return HTMLResponse(content=f"<h1>Error</h1><p>{str(e)}</p>")

@app.post("/api/parse")
async def parse_sql(request: SQLRequest):
    """Parse SQL query and return analysis information"""
    try:
        logger.info(f"Parsing SQL query: {request.query[:100]}...")
        
        # Parse the SQL query
        analysis = sql_parser.parse_query(request.query)
        logger.info(f"Parsed query type: {analysis.query_type}")
        logger.info(f"Source tables: {len(analysis.source_tables)}")
        logger.info(f"Target tables: {len(analysis.target_tables)}")
        logger.info(f"Columns: {len(analysis.columns)}")
        
        # Store analysis for lineage building
        query_analyses.append(analysis)
        
        # Build current lineage graph
        lineage_graph = lineage_builder.build_lineage(query_analyses)
        
        return {
            "success": True,
            "analysis": analysis.dict(),
            "lineage_graph": lineage_graph.dict(),
            "summary": lineage_builder.get_lineage_summary()
        }
        
    except Exception as e:
        logger.error(f"Error parsing SQL: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error parsing SQL: {str(e)}")

@app.get("/api/lineage")
async def get_lineage():
    """Get current lineage graph"""
    try:
        # Build lineage graph from all analyses
        lineage_graph = lineage_builder.build_lineage(query_analyses)
        summary = lineage_builder.get_lineage_summary()
        
        return {
            "success": True,
            "lineage_graph": lineage_graph.dict(),
            "summary": summary,
            "total_queries": len(query_analyses)
        }
    except Exception as e:
        logger.error(f"Error getting lineage: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting lineage: {str(e)}")

@app.get("/api/analyses")
async def get_analyses():
    """Get all query analyses"""
    try:
        return {
            "success": True,
            "analyses": [analysis.dict() for analysis in query_analyses],
            "total_count": len(query_analyses)
        }
    except Exception as e:
        logger.error(f"Error getting analyses: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting analyses: {str(e)}")

@app.get("/api/tables")
async def get_tables():
    """Get all discovered tables"""
    try:
        # Extract all unique tables from analyses
        tables = set()
        for analysis in query_analyses:
            for table in analysis.source_tables + analysis.target_tables:
                table_key = f"{table.database or ''}.{table.schema or ''}.{table.name}"
                tables.add(table_key)
        
        return {
            "success": True,
            "tables": list(tables),
            "total_count": len(tables)
        }
    except Exception as e:
        logger.error(f"Error getting tables: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting tables: {str(e)}")

@app.get("/api/schemas")
async def get_schemas():
    """Get all discovered schemas"""
    try:
        schemas = set()
        for analysis in query_analyses:
            for table in analysis.source_tables + analysis.target_tables:
                if table.schema:
                    schema_key = f"{table.database or ''}.{table.schema}"
                    schemas.add(schema_key)
        
        return {
            "success": True,
            "schemas": list(schemas),
            "total_count": len(schemas)
        }
    except Exception as e:
        logger.error(f"Error getting schemas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting schemas: {str(e)}")

@app.get("/api/columns")
async def get_columns():
    """Get all discovered columns"""
    try:
        columns = set()
        for analysis in query_analyses:
            for column in analysis.columns:
                column_key = f"{column.table or ''}.{column.name}"
                columns.add(column_key)
        
        return {
            "success": True,
            "columns": list(columns),
            "total_count": len(columns)
        }
    except Exception as e:
        logger.error(f"Error getting columns: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting columns: {str(e)}")

@app.get("/api/stats")
async def get_stats():
    """Get comprehensive statistics"""
    try:
        # Count different query types
        query_type_counts = {}
        for analysis in query_analyses:
            query_type = analysis.query_type.value
            query_type_counts[query_type] = query_type_counts.get(query_type, 0) + 1
        
        # Get lineage summary
        lineage_summary = lineage_builder.get_lineage_summary() if query_analyses else {
            'nodes': {'tables': 0, 'columns': 0, 'queries': 0, 'total': 0},
            'edges': {'by_type': {}, 'total': 0},
            'schemas': [],
            'databases': []
        }
        
        return {
            "success": True,
            "query_stats": {
                "total_queries": len(query_analyses),
                "by_type": query_type_counts
            },
            "lineage_stats": lineage_summary
        }
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting stats: {str(e)}")

@app.post("/api/upload")
async def upload_sql_file(file: UploadFile = File(...)):
    """Upload and parse SQL file"""
    try:
        # Read file content
        content = await file.read()
        sql_content = content.decode('utf-8')
        
        # Split into individual queries (simple split by semicolon)
        queries = [q.strip() for q in sql_content.split(';') if q.strip()]
        
        parsed_queries = []
        for query in queries:
            try:
                analysis = sql_parser.parse_query(query)
                query_analyses.append(analysis)
                parsed_queries.append(analysis.dict())
            except Exception as query_error:
                logger.warning(f"Failed to parse query: {query[:100]}... Error: {str(query_error)}")
                continue
        
        # Build lineage graph
        lineage_graph = lineage_builder.build_lineage(query_analyses)
        
        return {
            "success": True,
            "filename": file.filename,
            "total_queries": len(queries),
            "parsed_queries": len(parsed_queries),
            "failed_queries": len(queries) - len(parsed_queries),
            "lineage_graph": lineage_graph.dict(),
            "summary": lineage_builder.get_lineage_summary()
        }
        
    except Exception as e:
        logger.error(f"Error uploading file: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error uploading file: {str(e)}")

@app.delete("/api/reset")
async def reset_lineage():
    """Reset all lineage data"""
    try:
        global query_analyses
        query_analyses = []
        
        # Reset lineage builder
        global lineage_builder
        lineage_builder = LineageBuilder()
        
        return {
            "success": True, 
            "message": "All lineage data reset successfully"
        }
    except Exception as e:
        logger.error(f"Error resetting lineage: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error resetting lineage: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy", 
        "message": "SQL Lineage Tracker is running",
        "queries_parsed": len(query_analyses),
        "version": "1.0.0"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)