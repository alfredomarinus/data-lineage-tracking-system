from pydantic import BaseModel
from typing import List, Dict, Optional, Set
from enum import Enum

class QueryType(str, Enum):
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"

class TableInfo(BaseModel):
    name: str
    schema: Optional[str] = None  # Uncommented this line
    database: Optional[str] = None
    alias: Optional[str] = None
    type: str = "table"  # Added type field

class ColumnInfo(BaseModel):
    name: str
    table: Optional[str] = None
    alias: Optional[str] = None

class QueryAnalysis(BaseModel):
    query_type: QueryType
    source_tables: List[TableInfo]
    target_tables: List[TableInfo]
    columns: List[ColumnInfo]
    joins: List[Dict[str, str]]
    subqueries: List[str]

class LineageNode(BaseModel):
    id: str
    type: str
    name: str  # Added name field
    schema: Optional[str] = None  # Uncommented this line
    database: Optional[str] = None
    alias: Optional[str] = None  # Added alias field

class LineageEdge(BaseModel):
    source: str
    target: str
    type: str

class LineageGraph(BaseModel):
    nodes: List[LineageNode]
    edges: List[LineageEdge]
    metadata: Dict[str, str]

class SQLRequest(BaseModel):
    query: str
    database_type: Optional[str] = "postgresql"