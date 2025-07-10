from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import os
from pathlib import Path

from .models import SQLRequest, LineageGraph
from .parser import SQLParser
from .lineage import LineageBuilder

app = FastAPI(
    title="SQL Data Lineage Tracker",
    description="Parse SQL queries and visualize data lineage dependencies",
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

# Initialize components
sql_parser = SQLParser()
lineage_builder = LineageBuilder()

# Mount static files
static_dir = Path(__file__).parent.parent.parent / "frontend"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Serve the main HTML page"""
    html_file = static_dir / "index.html"
    if html_file.exists():
        return FileResponse(html_file)
    return HTMLResponse("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>SQL Data Lineage Tracker</title>
    </head>
    <body>
        <h1>SQL Data Lineage Tracker</h1>
        <p>Frontend files not found. Please ensure the frontend directory exists.</p>
    </body>
    </html>
    """)

@app.post("/api/parse", response_model=LineageGraph)
async def parse_sql(request: SQLRequest):
    """Parse SQL query and return lineage graph"""
    try:
        # Parse the SQL query
        analysis = sql_parser.parse_query(request.query)
        
        # Build lineage graph
        lineage_graph = lineage_builder.build_lineage([analysis])
        
        return lineage_graph
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error parsing SQL: {str(e)}")

@app.post("/api/parse-multiple", response_model=LineageGraph)
async def parse_multiple_sql(queries: List[str]):
    """Parse multiple SQL queries and return combined lineage graph"""
    try:
        analyses = []
        
        for query in queries:
            analysis = sql_parser.parse_query(query)
            analyses.append(analysis)
        
        # Build combined lineage graph
        lineage_graph = lineage_builder.build_lineage(analyses)
        
        return lineage_graph
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error parsing SQL queries: {str(e)}")

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "SQL Data Lineage Tracker"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)