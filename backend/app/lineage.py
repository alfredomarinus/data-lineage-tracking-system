from typing import List, Dict, Set
from .models import QueryAnalysis, LineageNode, LineageEdge, LineageGraph
import uuid

class LineageBuilder:
    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[LineageEdge] = []
        self.node_counter = 0
    
    def build_lineage(self, analyses: List[QueryAnalysis]) -> LineageGraph:
        """Build lineage graph from query analyses"""
        self.nodes = {}
        self.edges = []
        
        for analysis in analyses:
            self._process_analysis(analysis)
        
        return LineageGraph(
            nodes=list(self.nodes.values()),
            edges=self.edges,
            metadata={
                'total_queries': str(len(analyses)),
                'total_tables': str(len([n for n in self.nodes.values() if n.type == 'table'])),
                'total_columns': str(len([n for n in self.nodes.values() if n.type == 'column']))
            }
        )
    
    def _process_analysis(self, analysis: QueryAnalysis):
        """Process a single query analysis"""
        # Create nodes for source tables
        source_nodes = []
        for table in analysis.source_tables:
            node = self._create_table_node(table)
            source_nodes.append(node)
        
        # Create nodes for target tables
        target_nodes = []
        for table in analysis.target_tables:
            node = self._create_table_node(table)
            target_nodes.append(node)
        
        # Create nodes for columns
        column_nodes = []
        for column in analysis.columns:
            node = self._create_column_node(column)
            column_nodes.append(node)
        
        # Create query node
        query_node = self._create_query_node(analysis.query_type.value)
        
        # Create edges
        self._create_edges(source_nodes, target_nodes, column_nodes, query_node, analysis)
    
    def _create_table_node(self, table) -> LineageNode:
        """Create or get existing table node"""
        node_id = f"table_{table.database or 'default'}_{table.schema or 'public'}_{table.name}"
        
        if node_id not in self.nodes:
            self.nodes[node_id] = LineageNode(
                id=node_id,
                type='table',
                name=table.name,
                schema=table.schema,
                database=table.database
            )
        
        return self.nodes[node_id]
    
    def _create_column_node(self, column) -> LineageNode:
        """Create or get existing column node"""
        node_id = f"column_{column.table or 'unknown'}_{column.name}"
        
        if node_id not in self.nodes:
            self.nodes[node_id] = LineageNode(
                id=node_id,
                type='column',
                name=column.name,
                schema=column.table
            )
        
        return self.nodes[node_id]
    
    def _create_query_node(self, query_type: str) -> LineageNode:
        """Create query node"""
        self.node_counter += 1
        node_id = f"query_{query_type.lower()}_{self.node_counter}"
        
        node = LineageNode(
            id=node_id,
            type='query',
            name=f"{query_type} Query {self.node_counter}"
        )
        
        self.nodes[node_id] = node
        return node
    
    def _create_edges(self, source_nodes, target_nodes, column_nodes, query_node, analysis):
        """Create edges between nodes"""
        # Source tables to query
        for source_node in source_nodes:
            self.edges.append(LineageEdge(
                source=source_node.id,
                target=query_node.id,
                type='reads'
            ))
        
        # Query to target tables
        for target_node in target_nodes:
            self.edges.append(LineageEdge(
                source=query_node.id,
                target=target_node.id,
                type='writes'
            ))
        
        # Columns to query
        for column_node in column_nodes:
            self.edges.append(LineageEdge(
                source=column_node.id,
                target=query_node.id,
                type='uses'
            ))
        
        # Direct table-to-table edges for simple cases
        if len(source_nodes) == 1 and len(target_nodes) == 1:
            self.edges.append(LineageEdge(
                source=source_nodes[0].id,
                target=target_nodes[0].id,
                type='transforms'
            ))