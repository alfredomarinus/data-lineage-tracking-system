import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function, Parenthesis, Where, Comparison
from sqlparse.tokens import Keyword, DML, Name
from typing import List, Dict, Set, Optional, Tuple
import re
from .models import QueryAnalysis, TableInfo, ColumnInfo, QueryType

class SQLParser:
    def __init__(self):
        self.keywords = {
            'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 
            'RIGHT JOIN', 'FULL JOIN', 'OUTER JOIN', 'ON', 'INSERT', 'INTO',
            'UPDATE', 'SET', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TABLE',
            'VIEW', 'INDEX', 'UNION', 'INTERSECT', 'EXCEPT', 'WITH', 'AS'
        }
        
    def parse_query(self, query: str) -> QueryAnalysis:
        """Parse SQL query and extract lineage information"""
        # Clean and normalize query
        query = self._clean_query(query)
        
        # Parse with sqlparse
        parsed = sqlparse.parse(query)[0]
        
        # Extract query type
        query_type = self._extract_query_type(parsed)
        
        # Extract components based on query type
        if query_type == QueryType.SELECT:
            return self._parse_select_query(parsed, query)
        elif query_type == QueryType.INSERT:
            return self._parse_insert_query(parsed, query)
        elif query_type == QueryType.UPDATE:
            return self._parse_update_query(parsed, query)
        elif query_type == QueryType.DELETE:
            return self._parse_delete_query(parsed, query)
        else:
            return self._parse_ddl_query(parsed, query, query_type)
    
    def _clean_query(self, query: str) -> str:
        """Clean and normalize SQL query"""
        # Remove comments
        query = re.sub(r'--.*?\n', '\n', query)
        query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
        
        # Normalize whitespace
        query = re.sub(r'\s+', ' ', query).strip()
        
        return query
    
    def _extract_query_type(self, parsed) -> QueryType:
        """Extract the type of SQL query"""
        for token in parsed.tokens:
            if token.ttype is DML:
                return QueryType(token.value.upper())
            elif token.ttype is Keyword and token.value.upper() in ['CREATE', 'DROP', 'ALTER']:
                return QueryType(token.value.upper())
        return QueryType.SELECT
    
    def _parse_select_query(self, parsed, query: str) -> QueryAnalysis:
        """Parse SELECT query for lineage information"""
        source_tables = []
        columns = []
        joins = []
        subqueries = []
        
        # Extract FROM clause tables
        from_tables = self._extract_from_tables(parsed)
        source_tables.extend(from_tables)
        
        # Extract JOIN information
        join_info = self._extract_joins(parsed)
        joins.extend(join_info['joins'])
        source_tables.extend(join_info['tables'])
        
        # Extract SELECT columns
        select_columns = self._extract_select_columns(parsed)
        columns.extend(select_columns)
        
        # Extract subqueries
        subqueries = self._extract_subqueries(parsed)
        
        return QueryAnalysis(
            query_type=QueryType.SELECT,
            source_tables=self._deduplicate_tables(source_tables),
            target_tables=[],
            columns=columns,
            joins=joins,
            subqueries=subqueries
        )
    
    def _parse_insert_query(self, parsed, query: str) -> QueryAnalysis:
        """Parse INSERT query for lineage information"""
        target_tables = []
        source_tables = []
        columns = []
        
        # Extract target table
        target_table = self._extract_insert_target(parsed)
        if target_table:
            target_tables.append(target_table)
        
        # Extract source tables from SELECT part
        if 'SELECT' in query.upper():
            select_part = self._extract_select_from_insert(query)
            if select_part:
                select_analysis = self._parse_select_query(
                    sqlparse.parse(select_part)[0], select_part
                )
                source_tables.extend(select_analysis.source_tables)
                columns.extend(select_analysis.columns)
        
        return QueryAnalysis(
            query_type=QueryType.INSERT,
            source_tables=source_tables,
            target_tables=target_tables,
            columns=columns,
            joins=[],
            subqueries=[]
        )
    
    def _parse_update_query(self, parsed, query: str) -> QueryAnalysis:
        """Parse UPDATE query for lineage information"""
        target_tables = []
        source_tables = []
        columns = []
        
        # Extract target table
        target_table = self._extract_update_target(parsed)
        if target_table:
            target_tables.append(target_table)
            source_tables.append(target_table)  # UPDATE reads and writes to same table
        
        # Extract columns from SET clause
        set_columns = self._extract_set_columns(parsed)
        columns.extend(set_columns)
        
        return QueryAnalysis(
            query_type=QueryType.UPDATE,
            source_tables=source_tables,
            target_tables=target_tables,
            columns=columns,
            joins=[],
            subqueries=[]
        )
    
    def _parse_delete_query(self, parsed, query: str) -> QueryAnalysis:
        """Parse DELETE query for lineage information"""
        target_tables = []
        
        # Extract target table
        target_table = self._extract_delete_target(parsed)
        if target_table:
            target_tables.append(target_table)
        
        return QueryAnalysis(
            query_type=QueryType.DELETE,
            source_tables=target_tables,  # DELETE reads from target table
            target_tables=target_tables,
            columns=[],
            joins=[],
            subqueries=[]
        )
    
    def _parse_ddl_query(self, parsed, query: str, query_type: QueryType) -> QueryAnalysis:
        """Parse DDL queries (CREATE, DROP, ALTER)"""
        target_tables = []
        
        # Extract table name from DDL
        table_name = self._extract_ddl_table(parsed)
        if table_name:
            target_tables.append(TableInfo(name=table_name))
        
        return QueryAnalysis(
            query_type=query_type,
            source_tables=[],
            target_tables=target_tables,
            columns=[],
            joins=[],
            subqueries=[]
        )
    
    def _extract_from_tables(self, parsed) -> List[TableInfo]:
        """Extract tables from FROM clause"""
        tables = []
        from_seen = False
        
        for token in parsed.flatten():
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                from_seen = True
                continue
            
            if from_seen and token.ttype is None and not token.is_whitespace:
                if token.value.upper() not in self.keywords:
                    table_info = self._parse_table_identifier(token.value)
                    if table_info:
                        tables.append(table_info)
                        from_seen = False
        
        return tables
    
    def _extract_joins(self, parsed) -> Dict[str, List]:
        """Extract JOIN information"""
        joins = []
        tables = []
        
        tokens = list(parsed.flatten())
        for i, token in enumerate(tokens):
            if token.ttype is Keyword and 'JOIN' in token.value.upper():
                # Look for table name after JOIN
                for j in range(i + 1, len(tokens)):
                    next_token = tokens[j]
                    if next_token.ttype is None and not next_token.is_whitespace:
                        if next_token.value.upper() not in self.keywords:
                            table_info = self._parse_table_identifier(next_token.value)
                            if table_info:
                                tables.append(table_info)
                                joins.append({
                                    'type': token.value.upper(),
                                    'table': table_info.name
                                })
                            break
        
        return {'joins': joins, 'tables': tables}
    
    def _extract_select_columns(self, parsed) -> List[ColumnInfo]:
        """Extract columns from SELECT clause"""
        columns = []
        select_seen = False
        
        for token in parsed.flatten():
            if token.ttype is DML and token.value.upper() == 'SELECT':
                select_seen = True
                continue
            
            if select_seen and token.ttype is Keyword and token.value.upper() == 'FROM':
                break
            
            if select_seen and token.ttype is None and not token.is_whitespace:
                if token.value not in [',', '(', ')']:
                    column_info = self._parse_column_identifier(token.value)
                    if column_info:
                        columns.append(column_info)
        
        return columns
    
    def _extract_subqueries(self, parsed) -> List[str]:
        """Extract subqueries from the parsed SQL"""
        subqueries = []
        
        def extract_from_parenthesis(token):
            if isinstance(token, Parenthesis):
                content = str(token)[1:-1]  # Remove parentheses
                if any(keyword in content.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE']):
                    subqueries.append(content)
            elif hasattr(token, 'tokens'):
                for subtoken in token.tokens:
                    extract_from_parenthesis(subtoken)
        
        extract_from_parenthesis(parsed)
        return subqueries
    
    def _parse_table_identifier(self, identifier: str) -> Optional[TableInfo]:
        """Parse table identifier (schema.table or table)"""
        if not identifier or identifier.upper() in self.keywords:
            return None
        
        parts = identifier.split('.')
        if len(parts) == 1:
            return TableInfo(name=parts[0])
        elif len(parts) == 2:
            return TableInfo(schema=parts[0], name=parts[1])
        elif len(parts) == 3:
            return TableInfo(database=parts[0], schema=parts[1], name=parts[2])
        
        return None
    
    def _parse_column_identifier(self, identifier: str) -> Optional[ColumnInfo]:
        """Parse column identifier (table.column or column)"""
        if not identifier or identifier.upper() in self.keywords:
            return None
        
        if '.' in identifier:
            parts = identifier.split('.')
            if len(parts) == 2:
                return ColumnInfo(table=parts[0], name=parts[1])
        
        return ColumnInfo(name=identifier)
    
    def _extract_insert_target(self, parsed) -> Optional[TableInfo]:
        """Extract target table from INSERT statement"""
        into_seen = False
        
        for token in parsed.flatten():
            if token.ttype is Keyword and token.value.upper() == 'INTO':
                into_seen = True
                continue
            
            if into_seen and token.ttype is None and not token.is_whitespace:
                if token.value.upper() not in self.keywords:
                    return self._parse_table_identifier(token.value)
        
        return None
    
    def _extract_update_target(self, parsed) -> Optional[TableInfo]:
        """Extract target table from UPDATE statement"""
        update_seen = False
        
        for token in parsed.flatten():
            if token.ttype is DML and token.value.upper() == 'UPDATE':
                update_seen = True
                continue
            
            if update_seen and token.ttype is None and not token.is_whitespace:
                if token.value.upper() not in self.keywords:
                    return self._parse_table_identifier(token.value)
        
        return None
    
    def _extract_delete_target(self, parsed) -> Optional[TableInfo]:
        """Extract target table from DELETE statement"""
        from_seen = False
        
        for token in parsed.flatten():
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                from_seen = True
                continue
            
            if from_seen and token.ttype is None and not token.is_whitespace:
                if token.value.upper() not in self.keywords:
                    return self._parse_table_identifier(token.value)
        
        return None
    
    def _extract_ddl_table(self, parsed) -> Optional[str]:
        """Extract table name from DDL statement"""
        table_seen = False
        
        for token in parsed.flatten():
            if token.ttype is Keyword and token.value.upper() == 'TABLE':
                table_seen = True
                continue
            
            if table_seen and token.ttype is None and not token.is_whitespace:
                if token.value.upper() not in self.keywords:
                    return token.value
        
        return None
    
    def _extract_select_from_insert(self, query: str) -> Optional[str]:
        """Extract SELECT portion from INSERT statement"""
        select_match = re.search(r'SELECT.*', query, re.IGNORECASE | re.DOTALL)
        return select_match.group(0) if select_match else None
    
    def _extract_set_columns(self, parsed) -> List[ColumnInfo]:
        """Extract columns from SET clause in UPDATE"""
        columns = []
        set_seen = False
        
        for token in parsed.flatten():
            if token.ttype is Keyword and token.value.upper() == 'SET':
                set_seen = True
                continue
            
            if set_seen and token.ttype is Keyword and token.value.upper() == 'WHERE':
                break
            
            if set_seen and token.ttype is None and not token.is_whitespace:
                if '=' in token.value:
                    column_name = token.value.split('=')[0].strip()
                    column_info = self._parse_column_identifier(column_name)
                    if column_info:
                        columns.append(column_info)
        
        return columns
    
    def _deduplicate_tables(self, tables: List[TableInfo]) -> List[TableInfo]:
        """Remove duplicate tables from list"""
        seen = set()
        result = []
        
        for table in tables:
            key = f"{table.database or ''}.{table.schema or ''}.{table.name}"
            if key not in seen:
                seen.add(key)
                result.append(table)
        
        return result                                       