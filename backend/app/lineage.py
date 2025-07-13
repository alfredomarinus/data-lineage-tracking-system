from typing import List, Dict, Set, Optional
from .models import QueryAnalysis, LineageNode, LineageEdge, LineageGraph, TableInfo, ColumnInfo
import uuid

class LineageBuilder:
    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[LineageEdge] = []
        self.node_counter = 0
        self.table_column_mapping: Dict[str, Set[str]] = {}
        self.column_lineage: Dict[str, List[str]] = {}  # Track column dependencies
        
    def build_lineage(self, analyses: List[QueryAnalysis]) -> LineageGraph:
        """Build comprehensive lineage graph with column-level tracking"""
        self.nodes = {}
        self.edges = []
        self.node_counter = 0
        self.table_column_mapping = {}
        self.column_lineage = {}
        
        for analysis in analyses:
            self._process_analysis(analysis)
        
        # Create table-column relationships
        self._create_table_column_edges()
        
        # Create column-to-column lineage
        self._create_column_lineage_edges()
        
        return LineageGraph(
            nodes=list(self.nodes.values()),
            edges=self.edges,
            metadata={
                'total_queries': str(len(analyses)),
                'total_tables': str(len([n for n in self.nodes.values() if n.type == 'table'])),
                'total_columns': str(len([n for n in self.nodes.values() if n.type == 'column'])),
                'total_edges': str(len(self.edges)),
                'column_lineage_paths': str(len(self.column_lineage))
            }
        )
    
    def _process_analysis(self, analysis: QueryAnalysis):
        """Process a single query analysis with enhanced column tracking"""
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
        
        # Create nodes for columns with enhanced context
        column_nodes = []
        for column in analysis.columns:
            node = self._create_column_node(column, analysis.source_tables)
            column_nodes.append(node)
        
        # Create query node
        query_node = self._create_query_node(analysis.query_type.value)
        
        # Create enhanced edges with column lineage
        self._create_enhanced_edges(source_nodes, target_nodes, column_nodes, query_node, analysis)
        
        # Track column transformations
        self._track_column_transformations(analysis)
    
    def _create_enhanced_edges(self, source_nodes: List[LineageNode], target_nodes: List[LineageNode], 
                             column_nodes: List[LineageNode], query_node: LineageNode, analysis: QueryAnalysis):
        """Create enhanced edges with better column relationships"""
        
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
        
        # Enhanced column relationships based on query type
        if analysis.query_type.value == 'SELECT':
            self._create_select_column_edges(column_nodes, query_node, source_nodes)
        elif analysis.query_type.value == 'INSERT':
            self._create_insert_column_edges(column_nodes, query_node, source_nodes, target_nodes, analysis)
        elif analysis.query_type.value == 'UPDATE':
            self._create_update_column_edges(column_nodes, query_node, source_nodes, target_nodes, analysis)
        
        # Create data flow edges
        self._create_data_flow_edges(source_nodes, target_nodes, analysis)
        
        # Create join relationships
        self._create_join_edges(analysis)
    
    def _create_select_column_edges(self, column_nodes: List[LineageNode], query_node: LineageNode, source_nodes: List[LineageNode]):
        """Create edges for SELECT columns showing data flow"""
        for column_node in column_nodes:
            # Column participates in query
            self.edges.append(LineageEdge(
                source=column_node.id,
                target=query_node.id,
                type='selects'
            ))
            
            # Find source table for this column and create direct relationship
            for source_node in source_nodes:
                if self._column_belongs_to_table(column_node, source_node):
                    self.edges.append(LineageEdge(
                        source=source_node.id,
                        target=column_node.id,
                        type='provides'
                    ))
    
    def _create_insert_column_edges(self, column_nodes: List[LineageNode], query_node: LineageNode, 
                                   source_nodes: List[LineageNode], target_nodes: List[LineageNode], analysis: QueryAnalysis):
        """Create edges for INSERT showing column data flow"""
        for column_node in column_nodes:
            # For INSERT, columns flow from source to target through query
            self.edges.append(LineageEdge(
                source=column_node.id,
                target=query_node.id,
                type='sources'
            ))
            
            # If we have target tables, create column-to-target-table edges
            for target_node in target_nodes:
                # This represents the column being inserted into the target table
                target_column_id = f"column_{target_node.id.replace('table_', '')}_{column_node.name}"
                if target_column_id in self.nodes:
                    self.edges.append(LineageEdge(
                        source=column_node.id,
                        target=target_column_id,
                        type='flows_to'
                    ))
                else:
                    # Create implied target column
                    target_column = LineageNode(
                        id=target_column_id,
                        type='column',
                        name=column_node.name,
                        schema=target_node.schema,
                        database=target_node.database
                    )
                    self.nodes[target_column_id] = target_column
                    
                    self.edges.append(LineageEdge(
                        source=column_node.id,
                        target=target_column_id,
                        type='flows_to'
                    ))
                    
                    self.edges.append(LineageEdge(
                        source=target_node.id,
                        target=target_column_id,
                        type='contains'
                    ))
    
    def _create_update_column_edges(self, column_nodes: List[LineageNode], query_node: LineageNode,
                                   source_nodes: List[LineageNode], target_nodes: List[LineageNode], analysis: QueryAnalysis):
        """Create edges for UPDATE showing column modifications"""
        for column_node in column_nodes:
            # Column is being modified
            self.edges.append(LineageEdge(
                source=query_node.id,
                target=column_node.id,
                type='modifies'
            ))
            
            # Also show that the column is being read (for WHERE clauses, etc.)
            self.edges.append(LineageEdge(
                source=column_node.id,
                target=query_node.id,
                type='constrains'
            ))
    
    def _track_column_transformations(self, analysis: QueryAnalysis):
        """Track how columns are transformed across queries"""
        if analysis.query_type.value == 'INSERT' and analysis.source_tables and analysis.target_tables:
            # For INSERT with SELECT, track column flow
            for column in analysis.columns:
                source_column_key = self._get_column_key(column, analysis.source_tables)
                
                for target_table in analysis.target_tables:
                    target_column_key = f"{self._get_table_key(target_table)}.{column.name}"
                    
                    if source_column_key not in self.column_lineage:
                        self.column_lineage[source_column_key] = []
                    
                    self.column_lineage[source_column_key].append(target_column_key)
    
    def _create_column_lineage_edges(self):
        """Create edges showing column-to-column lineage"""
        for source_column_key, target_column_keys in self.column_lineage.items():
            source_column_id = f"column_{source_column_key.replace('.', '_')}"
            
            for target_column_key in target_column_keys:
                target_column_id = f"column_{target_column_key.replace('.', '_')}"
                
                if source_column_id in self.nodes and target_column_id in self.nodes:
                    self.edges.append(LineageEdge(
                        source=source_column_id,
                        target=target_column_id,
                        type='lineage'
                    ))
    
    def _column_belongs_to_table(self, column_node: LineageNode, table_node: LineageNode) -> bool:
        """Check if a column belongs to a specific table"""
        if column_node.schema == table_node.schema and column_node.database == table_node.database:
            # Extract table name from column node id
            column_table_part = column_node.id.replace('column_', '').split('_')[:-1]
            table_part = table_node.id.replace('table_', '').split('_')
            return '_'.join(column_table_part) == '_'.join(table_part)
        return False
    
    def _get_column_key(self, column: ColumnInfo, source_tables: List[TableInfo]) -> str:
        """Generate a unique key for a column"""
        table_info = self._resolve_column_table(column, source_tables)
        if table_info:
            return f"{self._get_table_key(table_info)}.{column.name}"
        return f"unknown.{column.name}"
    
    def get_column_lineage_summary(self) -> Dict:
        """Get summary of column-level lineage"""
        column_nodes = [n for n in self.nodes.values() if n.type == 'column']
        
        # Group columns by table
        columns_by_table = {}
        for column in column_nodes:
            table_key = self._extract_table_from_column_id(column.id)
            if table_key not in columns_by_table:
                columns_by_table[table_key] = []
            columns_by_table[table_key].append(column.name)
        
        # Count column relationships
        column_edges = [e for e in self.edges if e.type in ['flows_to', 'lineage', 'provides', 'sources']]
        
        return {
            'total_columns': len(column_nodes),
            'columns_by_table': columns_by_table,
            'column_relationships': len(column_edges),
            'column_lineage_paths': len(self.column_lineage),
            'relationship_types': {
                'flows_to': len([e for e in column_edges if e.type == 'flows_to']),
                'lineage': len([e for e in column_edges if e.type == 'lineage']),
                'provides': len([e for e in column_edges if e.type == 'provides']),
                'sources': len([e for e in column_edges if e.type == 'sources'])
            }
        }
    
    def _extract_table_from_column_id(self, column_id: str) -> str:
        """Extract table identifier from column ID"""
        # Remove 'column_' prefix and last part (column name)
        parts = column_id.replace('column_', '').split('_')
        return '_'.join(parts[:-1])
    
    # Include all other methods from the original LineageBuilder
    def _create_table_node(self, table: TableInfo) -> LineageNode:
        """Create or get existing table node"""
        table_key = self._get_table_key(table)
        node_id = f"table_{table_key}"
        
        if node_id not in self.nodes:
            self.nodes[node_id] = LineageNode(
                id=node_id,
                type='table',
                name=table.name,
                schema=table.schema,
                database=table.database,
                alias=table.alias
            )
            
            if table_key not in self.table_column_mapping:
                self.table_column_mapping[table_key] = set()
        
        return self.nodes[node_id]
    
    def _create_column_node(self, column: ColumnInfo, source_tables: List[TableInfo]) -> LineageNode:
        """Create or get existing column node"""
        table_info = self._resolve_column_table(column, source_tables)
        
        if table_info:
            table_key = self._get_table_key(table_info)
            node_id = f"column_{table_key}_{column.name}"
            
            if table_key not in self.table_column_mapping:
                self.table_column_mapping[table_key] = set()
            self.table_column_mapping[table_key].add(column.name)
        else:
            node_id = f"column_unknown_{column.name}"
        
        if node_id not in self.nodes:
            self.nodes[node_id] = LineageNode(
                id=node_id,
                type='column',
                name=column.name,
                schema=table_info.schema if table_info else None,
                database=table_info.database if table_info else None,
                alias=column.alias
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
    
    def _create_table_column_edges(self):
        """Create edges between tables and their columns"""
        for table_key, columns in self.table_column_mapping.items():
            table_id = f"table_{table_key}"
            if table_id in self.nodes:
                for column_name in columns:
                    column_id = f"column_{table_key}_{column_name}"
                    if column_id in self.nodes:
                        self.edges.append(LineageEdge(
                            source=table_id,
                            target=column_id,
                            type='contains'
                        ))
    
    def _create_data_flow_edges(self, source_nodes: List[LineageNode], 
                               target_nodes: List[LineageNode], analysis: QueryAnalysis):
        """Create direct data flow edges between tables"""
        if analysis.query_type.value in ['INSERT', 'UPDATE'] and source_nodes and target_nodes:
            for source_node in source_nodes:
                for target_node in target_nodes:
                    if source_node.id != target_node.id or analysis.query_type.value == 'UPDATE':
                        edge_type = 'transforms' if analysis.query_type.value == 'INSERT' else 'updates'
                        self.edges.append(LineageEdge(
                            source=source_node.id,
                            target=target_node.id,
                            type=edge_type
                        ))
    
    def _create_join_edges(self, analysis: QueryAnalysis):
        """Create edges for JOIN relationships"""
        if analysis.joins:
            for join_info in analysis.joins:
                join_table = join_info.get('table')
                if join_table:
                    for table in analysis.source_tables:
                        if table.name == join_table:
                            continue
                        
                        table1_key = self._get_table_key(table)
                        table1_id = f"table_{table1_key}"
                        
                        for other_table in analysis.source_tables:
                            if other_table.name == join_table:
                                table2_key = self._get_table_key(other_table)
                                table2_id = f"table_{table2_key}"
                                
                                if table1_id in self.nodes and table2_id in self.nodes:
                                    self.edges.append(LineageEdge(
                                        source=table1_id,
                                        target=table2_id,
                                        type=f"joins_{join_info.get('type', 'JOIN').lower()}"
                                    ))
                                break
    
    def _resolve_column_table(self, column: ColumnInfo, source_tables: List[TableInfo]) -> Optional[TableInfo]:
        """Resolve which table a column belongs to"""
        if column.table:
            for table in source_tables:
                if (table.name == column.table or 
                    table.alias == column.table or 
                    (table.schema and f"{table.schema}.{table.name}" == column.table)):
                    return table
        
        return source_tables[0] if source_tables else None
    
    def _get_table_key(self, table: TableInfo) -> str:
        """Generate a unique key for a table"""
        parts = []
        if table.database:
            parts.append(table.database)
        if table.schema:
            parts.append(table.schema)
        parts.append(table.name)
        
        if table.alias:
            parts.append(f"as_{table.alias}")
        
        return "_".join(parts)
    
    def get_lineage_summary(self) -> Dict:
        """Get enhanced summary including column lineage"""
        table_nodes = [n for n in self.nodes.values() if n.type == 'table']
        column_nodes = [n for n in self.nodes.values() if n.type == 'column']
        query_nodes = [n for n in self.nodes.values() if n.type == 'query']
        
        edge_types = {}
        for edge in self.edges:
            edge_types[edge.type] = edge_types.get(edge.type, 0) + 1
        
        return {
            'nodes': {
                'tables': len(table_nodes),
                'columns': len(column_nodes),
                'queries': len(query_nodes),
                'total': len(self.nodes)
            },
            'edges': {
                'by_type': edge_types,
                'total': len(self.edges)
            },
            'schemas': list(set(n.schema for n in table_nodes if n.schema)),
            'databases': list(set(n.database for n in table_nodes if n.database)),
            'column_lineage': self.get_column_lineage_summary()
        }