class SQLLineageParser {
    constructor() {
        this.relationships = [];
        this.tables = [];
        this.columns = [];
    }
    
    parseSQL(sqlQuery) {
        this.relationships = [];
        this.tables = [];
        this.columns = [];
        
        const cleanQuery = sqlQuery.trim().replace(/\s+/g, ' ');
        
        // Extract tables and columns
        this.extractTablesAndColumns(cleanQuery);
        
        // Generate relationships
        this.generateRelationships(cleanQuery);
        
        return {
            query: cleanQuery,
            relationships: this.relationships,
            tables: this.tables,
            columns: this.columns
        };
    }
    
    extractTablesAndColumns(sql) {
        const sqlUpper = sql.toUpperCase();
        
        // Extract SELECT columns
        const selectMatch = sql.match(/SELECT\s+(.*?)\s+FROM/si);
        if (selectMatch) {
            const selectClause = selectMatch[1];
            this.columns = this.parseSelectClause(selectClause);
        }
        
        // Extract tables from FROM clause
        const fromMatch = sql.match(/FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)/i);
        if (fromMatch) {
            const tableName = fromMatch[1].split('.').pop(); // Remove schema if present
            this.tables.push(tableName);
        }
        
        // Extract tables from JOIN clauses
        const joinRegex = /(?:INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)/gi;
        let joinMatch;
        while ((joinMatch = joinRegex.exec(sql)) !== null) {
            const tableName = joinMatch[1].split('.').pop(); // Remove schema if present
            if (!this.tables.includes(tableName)) {
                this.tables.push(tableName);
            }
        }
        
        // Extract columns from WHERE clause
        const whereColumns = this.extractWhereColumns(sql);
        whereColumns.forEach(col => {
            if (!this.columns.includes(col)) {
                this.columns.push(col);
            }
        });
        
        // Extract columns from ORDER BY clause
        const orderByColumns = this.extractOrderByColumns(sql);
        orderByColumns.forEach(col => {
            if (!this.columns.includes(col)) {
                this.columns.push(col);
            }
        });
        
        // Extract columns from GROUP BY clause
        const groupByColumns = this.extractGroupByColumns(sql);
        groupByColumns.forEach(col => {
            if (!this.columns.includes(col)) {
                this.columns.push(col);
            }
        });
    }
    
    parseSelectClause(selectClause) {
        const columns = [];
        
        // Handle SELECT *
        if (selectClause.trim() === '*') {
            return ['*'];
        }
        
        // Split by comma, but be careful with functions
        const parts = this.splitSelectClause(selectClause);
        
        for (let part of parts) {
            part = part.trim();
            
            // Skip empty parts
            if (!part) continue;
            
            // Handle AS aliases
            if (part.toUpperCase().includes(' AS ')) {
                const asMatch = part.match(/^(.*?)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)/i);
                if (asMatch) {
                    const columnExpr = asMatch[1].trim();
                    const alias = asMatch[2].trim();
                    
                    // Extract column from expression
                    const exprColumns = this.extractColumnsFromExpression(columnExpr);
                    columns.push(...exprColumns);
                    continue;
                }
            }
            
            // Handle functions like COUNT(), SUM(), etc.
            if (part.includes('(') && part.includes(')')) {
                const funcColumns = this.extractColumnsFromExpression(part);
                columns.push(...funcColumns);
                continue;
            }
            
            // Handle table.column format
            if (part.includes('.')) {
                const columnName = part.split('.').pop();
                columns.push(columnName);
                continue;
            }
            
            // Regular column name
            if (this.isValidColumnName(part)) {
                columns.push(part);
            }
        }
        
        return [...new Set(columns)]; // Remove duplicates
    }
    
    splitSelectClause(selectClause) {
        const parts = [];
        let current = '';
        let parenCount = 0;
        
        for (let i = 0; i < selectClause.length; i++) {
            const char = selectClause[i];
            
            if (char === '(') {
                parenCount++;
            } else if (char === ')') {
                parenCount--;
            } else if (char === ',' && parenCount === 0) {
                parts.push(current.trim());
                current = '';
                continue;
            }
            
            current += char;
        }
        
        if (current.trim()) {
            parts.push(current.trim());
        }
        
        return parts;
    }
    
    extractColumnsFromExpression(expr) {
        const columns = [];
        
        // Remove function names and parentheses, but keep the content
        const cleanExpr = expr.replace(/[a-zA-Z_][a-zA-Z0-9_]*\s*\(/g, '(');
        
        // Extract column references
        const columnRegex = /(?:^|[^a-zA-Z0-9_])([a-zA-Z_][a-zA-Z0-9_]*\.)?([a-zA-Z_][a-zA-Z0-9_]*)/g;
        let match;
        
        while ((match = columnRegex.exec(cleanExpr)) !== null) {
            const columnName = match[2];
            
            // Skip SQL keywords and functions
            if (!this.isSQLKeyword(columnName) && this.isValidColumnName(columnName)) {
                columns.push(columnName);
            }
        }
        
        return columns;
    }
    
    extractWhereColumns(sql) {
        const whereMatch = sql.match(/WHERE\s+(.*?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+HAVING|\s+LIMIT|\s*;|\s*$)/si);
        if (!whereMatch) return [];
        
        const whereClause = whereMatch[1];
        return this.extractColumnsFromExpression(whereClause);
    }
    
    extractOrderByColumns(sql) {
        const orderByMatch = sql.match(/ORDER\s+BY\s+(.*?)(?:\s+LIMIT|\s*;|\s*$)/si);
        if (!orderByMatch) return [];
        
        const orderByClause = orderByMatch[1];
        const columns = [];
        
        // Split by comma and extract column names
        const parts = orderByClause.split(',');
        for (let part of parts) {
            part = part.trim().replace(/\s+(ASC|DESC)$/i, '');
            if (part.includes('.')) {
                columns.push(part.split('.').pop());
            } else if (this.isValidColumnName(part)) {
                columns.push(part);
            }
        }
        
        return columns;
    }
    
    extractGroupByColumns(sql) {
        const groupByMatch = sql.match(/GROUP\s+BY\s+(.*?)(?:\s+HAVING|\s+ORDER\s+BY|\s+LIMIT|\s*;|\s*$)/si);
        if (!groupByMatch) return [];
        
        const groupByClause = groupByMatch[1];
        const columns = [];
        
        // Split by comma and extract column names
        const parts = groupByClause.split(',');
        for (let part of parts) {
            part = part.trim();
            if (part.includes('.')) {
                columns.push(part.split('.').pop());
            } else if (this.isValidColumnName(part)) {
                columns.push(part);
            }
        }
        
        return columns;
    }
    
    generateRelationships(sql) {
        // For each table, create "provides" relationships to all columns
        for (const table of this.tables) {
            for (const column of this.columns) {
                // Skip if column is '*'
                if (column === '*') continue;
                
                this.relationships.push({
                    source: table,
                    target: column,
                    type: 'provides'
                });
            }
        }
        
        // Additional relationships based on SQL type
        const sqlUpper = sql.toUpperCase();
        
        // Handle UPDATE queries
        if (sqlUpper.includes('UPDATE')) {
            // Add "modifies" relationships
            // Implementation depends on specific requirements
        }
        
        // Handle INSERT queries
        if (sqlUpper.includes('INSERT')) {
            // Add "modifies" relationships
            // Implementation depends on specific requirements
        }
    }
    
    // Fixed isValidColumnName method to be more permissive
    isValidColumnName(name) {
        if (!name || typeof name !== 'string') return false;
        
        // Remove any whitespace
        name = name.trim();
        
        // Check if it's a valid column name (starts with letter or underscore)
        const isValidFormat = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name);
        
        // Check if it's not a SQL keyword
        const isNotKeyword = !this.isSQLKeyword(name);
        
        return isValidFormat && isNotKeyword;
    }
    
    isSQLKeyword(word) {
        const keywords = [
            'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL',
            'ON', 'AND', 'OR', 'NOT', 'NULL', 'TRUE', 'FALSE', 'COUNT', 'SUM',
            'AVG', 'MIN', 'MAX', 'GROUP', 'BY', 'ORDER', 'HAVING', 'LIMIT',
            'DISTINCT', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'LIKE',
            'IN', 'BETWEEN', 'EXISTS', 'ALL', 'ANY', 'SOME', 'UNION', 'INTERSECT',
            'EXCEPT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP',
            'TABLE', 'VIEW', 'INDEX', 'DATABASE', 'SCHEMA', 'PRIMARY', 'KEY',
            'FOREIGN', 'REFERENCES', 'CONSTRAINT', 'UNIQUE', 'CHECK', 'DEFAULT',
            'AUTO_INCREMENT', 'IDENTITY', 'TIMESTAMP', 'DATETIME', 'DATE', 'TIME',
            'VARCHAR', 'CHAR', 'TEXT', 'INT', 'INTEGER', 'BIGINT', 'SMALLINT',
            'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'BOOLEAN', 'BOOL'
        ];
        
        return keywords.includes(word.toUpperCase());
    }
    
    formatOutput(result) {
        let output = `query:\n${result.query}\n\n`;
        
        if (result.relationships.length > 0) {
            output += 'relationship:\n';
            for (const rel of result.relationships) {
                output += `[${rel.source}] --${rel.type}--> [${rel.target}]\n`;
            }
        }
        
        return output;
    }
}

class SQLLineageVisualizer {
    constructor() {
        this.apiUrl = '/api';
        this.currentData = null;
        this.svg = null;
        this.simulation = null;
        this.lineageParser = new SQLLineageParser();
        
        this.init();
    }
    
    init() {
        this.setupEventListeners();
        this.setupTabs();
        this.setupRelationshipOutput();
    }
    
    setupEventListeners() {
        document.getElementById('parse-btn').addEventListener('click', () => this.parseSQL());
        document.getElementById('clear-btn').addEventListener('click', () => this.clearAll());
        document.getElementById('example-btn').addEventListener('click', () => this.loadExample());
        
        // Enter key in textarea
        document.getElementById('sql-input').addEventListener('keydown', (e) => {
            if (e.ctrlKey && e.key === 'Enter') {
                this.parseSQL();
            }
        });
    }
    
    setupTabs() {
        const tabButtons = document.querySelectorAll('.tab-btn');
        const tabPanes = document.querySelectorAll('.tab-pane');
        
        tabButtons.forEach(button => {
            button.addEventListener('click', () => {
                const tabId = button.dataset.tab;
                
                // Update active tab button
                tabButtons.forEach(btn => btn.classList.remove('active'));
                button.classList.add('active');
                
                // Update active tab pane
                tabPanes.forEach(pane => pane.classList.remove('active'));
                document.getElementById(tabId).classList.add('active');
            });
        });
    }
    
    async parseSQL() {
        const sqlInput = document.getElementById('sql-input').value.trim();
        
        if (!sqlInput) {
            this.showError('Please enter a SQL query');
            return;
        }
        
        this.showLoading(true);
        
        try {
            const response = await fetch(`${this.apiUrl}/parse`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ query: sqlInput })
            });
            
            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.detail || 'Failed to parse SQL');
            }
            
            const data = await response.json();
            
            // Validate data structure
            const validatedData = this.validateAndNormalizeData(data);
            this.currentData = validatedData;
            
            this.visualizeData(validatedData);
            this.updateDetails(validatedData);
            this.updateJSON(validatedData);
            
            // Switch to visualization tab
            const vizTab = document.querySelector('.tab-btn[data-tab="visualization"]');
            if (vizTab) {
                vizTab.click();
            }
            
        } catch (error) {
            console.error('Parse error:', error);
            this.showError(error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    validateAndNormalizeData(data) {
        // Ensure data has required structure
        const normalized = {
            nodes: [],
            edges: []
        };
        
        // Handle different possible data structures
        if (data.nodes && Array.isArray(data.nodes)) {
            normalized.nodes = data.nodes.map(node => ({
                id: node.id || node.name || `node_${Math.random()}`,
                name: node.name || node.id || 'Unnamed',
                type: node.type || 'unknown',
                schema: node.schema || null,
                ...node
            }));
        } else if (data.tables || data.columns || data.queries) {
            // Handle separate arrays
            let nodeId = 0;
            
            if (data.tables && Array.isArray(data.tables)) {
                data.tables.forEach(table => {
                    normalized.nodes.push({
                        id: table.id || `table_${nodeId++}`,
                        name: table.name || table.table_name || 'Unknown Table',
                        type: 'table',
                        schema: table.schema || null
                    });
                });
            }
            
            if (data.columns && Array.isArray(data.columns)) {
                data.columns.forEach(column => {
                    normalized.nodes.push({
                        id: column.id || `column_${nodeId++}`,
                        name: column.name || column.column_name || 'Unknown Column',
                        type: 'column',
                        schema: column.schema || null,
                        table: column.table || null
                    });
                });
            }
            
            if (data.queries && Array.isArray(data.queries)) {
                data.queries.forEach(query => {
                    normalized.nodes.push({
                        id: query.id || `query_${nodeId++}`,
                        name: query.name || query.query_name || 'Query',
                        type: 'query',
                        schema: query.schema || null
                    });
                });
            }
        }
        
        // Handle edges/relationships
        if (data.edges && Array.isArray(data.edges)) {
            normalized.edges = data.edges.map(edge => ({
                source: edge.source || edge.from || edge.sourceId,
                target: edge.target || edge.to || edge.targetId,
                type: edge.type || edge.relationship || 'uses',
                ...edge
            }));
        } else if (data.relationships && Array.isArray(data.relationships)) {
            normalized.edges = data.relationships.map(rel => ({
                source: rel.source || rel.from || rel.sourceId,
                target: rel.target || rel.to || rel.targetId,
                type: rel.type || rel.relationship || 'uses'
            }));
        }
        
        // If no nodes found, create sample data from SQL input
        if (normalized.nodes.length === 0) {
            this.createSampleDataFromSQL(normalized);
        }
        
        return normalized;
    }
    
    // Updated methods for the SQLLineageVisualizer class
    createSampleDataFromSQL(normalized) {
        const sqlInput = document.getElementById('sql-input').value;
        
        // Use the lineage parser to extract tables and columns
        const result = this.lineageParser.parseSQL(sqlInput);
        
        // Extract table information with proper alias handling
        const tableInfo = this.extractTableInfoWithAliases(sqlInput);
        
        // Add tables (use actual table names, not aliases)
        const uniqueTables = [...new Set(tableInfo.map(t => t.tableName))];
        uniqueTables.forEach((table, index) => {
            normalized.nodes.push({
                id: `table_${index}`,
                name: table,
                type: 'table'
            });
        });
        
        // Add columns (including aliases)
        const columnsWithAliases = this.extractColumnsWithAliases(sqlInput);
        columnsWithAliases.forEach((column, index) => {
            normalized.nodes.push({
                id: `column_${index}`,
                name: column,
                type: 'column'
            });
        });
        
        // Add main query
        if (uniqueTables.length > 0 || columnsWithAliases.length > 0) {
            normalized.nodes.push({
                id: 'main_query',
                name: 'Main Query',
                type: 'query'
            });
        }
        
        // Create relationships based on SQL patterns
        this.createRelationshipsFromSQL(normalized, sqlInput, uniqueTables, columnsWithAliases);
    }

    // New method to extract table information with aliases
    extractTableInfoWithAliases(sql) {
        const tableInfo = [];
        
        // Extract FROM clause table and alias
        const fromMatch = sql.match(/FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\s*(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)?/i);
        if (fromMatch) {
            const fullTableName = fromMatch[1];
            const tableName = fullTableName.split('.').pop(); // Remove schema if present
            const alias = fromMatch[2] || tableName;
            tableInfo.push({
                tableName: tableName,
                alias: alias.toLowerCase(),
                fullName: fullTableName
            });
        }
        
        // Extract JOIN clauses with aliases
        const joinRegex = /(?:INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\s*(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)?/gi;
        let joinMatch;
        
        while ((joinMatch = joinRegex.exec(sql)) !== null) {
            const fullTableName = joinMatch[1];
            const tableName = fullTableName.split('.').pop(); // Remove schema if present
            const alias = joinMatch[2] || tableName;
            
            // Check if this table is already in the list
            const exists = tableInfo.some(t => t.tableName === tableName);
            if (!exists) {
                tableInfo.push({
                    tableName: tableName,
                    alias: alias.toLowerCase(),
                    fullName: fullTableName
                });
            }
        }
        
        return tableInfo;
    }

    // Updated extractColumnsWithAliases method
    extractColumnsWithAliases(sql) {
        const columnRegex = /SELECT\s+(.*?)\s+FROM/si;
        const match = sql.match(columnRegex);
        if (!match) return [];
        
        const selectClause = match[1];
        const columns = [];
        
        // Get table info for resolving aliases
        const tableInfo = this.extractTableInfoWithAliases(sql);
        const aliasToTableMap = {};
        tableInfo.forEach(t => {
            aliasToTableMap[t.alias] = t.tableName;
        });
        
        // Split select clause properly
        const parts = this.splitSelectClause(selectClause);
        
        parts.forEach(part => {
            const trimmedPart = part.trim();
            
            // Skip if it's just SELECT *
            if (trimmedPart === '*') {
                return;
            }
            
            // Handle aliases (AS keyword)
            if (trimmedPart.toUpperCase().includes(' AS ')) {
                const asMatch = trimmedPart.match(/^(.*?)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)/i);
                if (asMatch) {
                    const sourceExpr = asMatch[1].trim();
                    const alias = asMatch[2].trim();
                    
                    // Add the alias as a column
                    columns.push(alias);
                    
                    // Also extract the source column if it's a simple table.column reference
                    const sourceColumns = this.extractColumnNamesFromExpression(sourceExpr, aliasToTableMap);
                    sourceColumns.forEach(col => {
                        if (!columns.includes(col)) {
                            columns.push(col);
                        }
                    });
                    return;
                }
            }
            
            // Handle table.column references (with or without aliases)
            const columnNames = this.extractColumnNamesFromExpression(trimmedPart, aliasToTableMap);
            columnNames.forEach(col => {
                if (!columns.includes(col)) {
                    columns.push(col);
                }
            });
        });
        
        return [...new Set(columns)];
    }

    // New helper method to extract column names from expressions
    extractColumnNamesFromExpression(expression, aliasToTableMap) {
        const columns = [];
        
        // Handle table.column references (with potential aliases)
        const tableColumnRegex = /([a-zA-Z_][a-zA-Z0-9_]*)\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]*)/g;
        let match;
        
        while ((match = tableColumnRegex.exec(expression)) !== null) {
            const tableOrAlias = match[1].toLowerCase();
            const columnName = match[2];
            
            // The column name is what we want to track, regardless of the table/alias
            columns.push(columnName);
        }
        
        // Handle simple column references (no table prefix)
        if (columns.length === 0) {
            // Check if it's a simple column name (not a function or complex expression)
            const simpleColumnMatch = expression.match(/^([a-zA-Z_][a-zA-Z0-9_]*)$/);
            if (simpleColumnMatch) {
                columns.push(simpleColumnMatch[1]);
            }
        }
        
        return columns;
    }

    // Updated createRelationshipsFromSQL method
    createRelationshipsFromSQL(normalized, sql, tables, columns) {
        const sqlUpper = sql.toUpperCase();
        
        // Get table info with aliases
        const tableInfo = this.extractTableInfoWithAliases(sql);
        
        // Parse the SQL to extract table-column relationships more accurately
        const tableColumnMap = this.parseTableColumnRelationshipsWithAliases(sql, tableInfo);
        
        // 1. Create accurate table-provides-column relationships
        Object.entries(tableColumnMap).forEach(([tableName, tableColumns]) => {
            const tableIndex = tables.findIndex(t => t.toLowerCase() === tableName.toLowerCase());
            if (tableIndex !== -1) {
                tableColumns.forEach(columnName => {
                    const columnIndex = columns.findIndex(c => c.toLowerCase() === columnName.toLowerCase());
                    if (columnIndex !== -1) {
                        // FIXED: Ensure this relationship is created
                        const existingRelationship = normalized.edges.find(e => 
                            e.source === `table_${tableIndex}` && 
                            e.target === `column_${columnIndex}` && 
                            e.type === 'provides'
                        );
                        
                        if (!existingRelationship) {
                            normalized.edges.push({
                                source: `table_${tableIndex}`,
                                target: `column_${columnIndex}`,
                                type: 'provides'
                            });
                        }
                    }
                });
            }
        });
        
        // FIXED: If no relationships found yet, create default relationships
        // This handles simple cases like "SELECT column_a FROM table_a"
        if (normalized.edges.length === 0 && tables.length > 0 && columns.length > 0) {
            // For simple queries, assume all columns come from the first table
            const mainTableIndex = 0;
            columns.forEach((column, columnIndex) => {
                if (column !== '*') {
                    normalized.edges.push({
                        source: `table_${mainTableIndex}`,
                        target: `column_${columnIndex}`,
                        type: 'provides'
                    });
                }
            });
        }
        
        // 2. Columns flow to query (SELECT columns)
        if (normalized.nodes.find(n => n.id === 'main_query')) {
            columns.forEach((column, columnIndex) => {
                normalized.edges.push({
                    source: `column_${columnIndex}`,
                    target: 'main_query',
                    type: 'flows_to'
                });
            });
        }
        
        // 3. Tables source the query (FROM/JOIN tables)
        tables.forEach((table, tableIndex) => {
            if (normalized.nodes.find(n => n.id === 'main_query')) {
                normalized.edges.push({
                    source: `table_${tableIndex}`,
                    target: 'main_query',
                    type: 'sources'
                });
            }
        });
        
        // Rest of the method remains the same...
        // (WHERE, UPDATE, INSERT handling)
    }

    // New helper method to parse table-column relationships with proper alias handling
    parseTableColumnRelationshipsWithAliases(sql, tableInfo) {
        const tableColumnMap = {};
        
        // Create alias to table name mapping
        const aliasToTableMap = {};
        tableInfo.forEach(t => {
            aliasToTableMap[t.alias] = t.tableName;
        });
        
        // Extract SELECT clause
        const selectMatch = sql.match(/SELECT\s+(.*?)\s+FROM/si);
        if (!selectMatch) return tableColumnMap;
        
        const selectClause = selectMatch[1];
        
        // Parse each column in SELECT clause
        const columns = this.splitSelectClause(selectClause);
        
        columns.forEach(column => {
            const trimmedColumn = column.trim();
            
            // Skip SELECT *
            if (trimmedColumn === '*') {
                // For SELECT *, add all columns from all tables
                tableInfo.forEach(t => {
                    if (!tableColumnMap[t.tableName]) {
                        tableColumnMap[t.tableName] = [];
                    }
                    tableColumnMap[t.tableName].push('*');
                });
                return;
            }
            
            // Extract table.column references from the column expression
            this.extractTableColumnReferencesWithAliases(trimmedColumn, aliasToTableMap, tableColumnMap);
            
            // FIXED: Handle unqualified column names
            // If no table.column references found, assume column comes from first table
            if (!trimmedColumn.includes('.') && this.isValidColumnName(trimmedColumn)) {
                // Get the first table (main table from FROM clause)
                const mainTable = tableInfo.length > 0 ? tableInfo[0].tableName : null;
                if (mainTable) {
                    if (!tableColumnMap[mainTable]) {
                        tableColumnMap[mainTable] = [];
                    }
                    if (!tableColumnMap[mainTable].includes(trimmedColumn)) {
                        tableColumnMap[mainTable].push(trimmedColumn);
                    }
                }
            }
        });
        
        // Also extract columns from WHERE, GROUP BY, ORDER BY clauses
        this.extractColumnsFromClausesWithAliases(sql, aliasToTableMap, tableColumnMap);
        
        return tableColumnMap;
    }

    extractTableColumnReferencesWithAliases(expression, aliasToTableMap, tableColumnMap) {
        // Regular expression to match table.column patterns
        const tableColumnRegex = /([a-zA-Z_][a-zA-Z0-9_]*)\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]*)/g;
        let match;
        
        while ((match = tableColumnRegex.exec(expression)) !== null) {
            const tableOrAlias = match[1].toLowerCase();
            const columnName = match[2];
            
            // Resolve table alias to actual table name
            const actualTableName = aliasToTableMap[tableOrAlias] || tableOrAlias;
            
            if (!tableColumnMap[actualTableName]) {
                tableColumnMap[actualTableName] = [];
            }
            
            if (!tableColumnMap[actualTableName].includes(columnName)) {
                tableColumnMap[actualTableName].push(columnName);
            }
        }
    }

    // Updated method to extract columns from clauses with proper alias handling
    extractColumnsFromClausesWithAliases(sql, aliasToTableMap, tableColumnMap) {
        // Extract from WHERE clause
        const whereMatch = sql.match(/WHERE\s+(.*?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+HAVING|\s+LIMIT|\s*;|\s*$)/si);
        if (whereMatch) {
            this.extractTableColumnReferencesWithAliases(whereMatch[1], aliasToTableMap, tableColumnMap);
        }
        
        // Extract from GROUP BY clause
        const groupByMatch = sql.match(/GROUP\s+BY\s+(.*?)(?:\s+HAVING|\s+ORDER\s+BY|\s+LIMIT|\s*;|\s*$)/si);
        if (groupByMatch) {
            this.extractTableColumnReferencesWithAliases(groupByMatch[1], aliasToTableMap, tableColumnMap);
        }
        
        // Extract from ORDER BY clause
        const orderByMatch = sql.match(/ORDER\s+BY\s+(.*?)(?:\s+LIMIT|\s*;|\s*$)/si);
        if (orderByMatch) {
            this.extractTableColumnReferencesWithAliases(orderByMatch[1], aliasToTableMap, tableColumnMap);
        }
        
        // Extract from HAVING clause
        const havingMatch = sql.match(/HAVING\s+(.*?)(?:\s+ORDER\s+BY|\s+LIMIT|\s*;|\s*$)/si);
        if (havingMatch) {
            this.extractTableColumnReferencesWithAliases(havingMatch[1], aliasToTableMap, tableColumnMap);
        }
        
        // Extract from JOIN conditions
        const joinRegex = /(?:INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|JOIN)\s+[a-zA-Z_][a-zA-Z0-9_]*(?:\s+(?:AS\s+)?[a-zA-Z_][a-zA-Z0-9_]*)?\s+ON\s+(.*?)(?=\s+(?:INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|JOIN)|WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|;|$)/gi;
        let joinMatch;
        
        while ((joinMatch = joinRegex.exec(sql)) !== null) {
            this.extractTableColumnReferencesWithAliases(joinMatch[1], aliasToTableMap, tableColumnMap);
        }
    }

    // New helper method to parse table-column relationships from SQL
    parseTableColumnRelationships(sql) {
        const tableColumnMap = {};
        
        // Extract SELECT clause
        const selectMatch = sql.match(/SELECT\s+(.*?)\s+FROM/si);
        if (!selectMatch) return tableColumnMap;
        
        const selectClause = selectMatch[1];
        
        // Extract FROM clause and JOINs to get table aliases
        const tableAliases = this.extractTableAliases(sql);
        
        // Parse each column in SELECT clause
        const columns = this.splitSelectClause(selectClause);
        
        columns.forEach(column => {
            const trimmedColumn = column.trim();
            
            // Skip aggregate functions without table qualification
            if (trimmedColumn === '*') return;
            
            // Extract all table.column references from the column expression
            this.extractTableColumnReferences(trimmedColumn, tableAliases, tableColumnMap);
        });
        
        // Also extract columns from WHERE, GROUP BY, ORDER BY clauses
        this.extractColumnsFromClauses(sql, tableAliases, tableColumnMap);
        
        return tableColumnMap;
    }

    // Helper method to extract table.column references from any expression
    extractTableColumnReferences(expression, tableAliases, tableColumnMap) {
        // Regular expression to match table.column patterns
        const tableColumnRegex = /([a-zA-Z_][a-zA-Z0-9_]*)\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]*)/g;
        let match;
        
        while ((match = tableColumnRegex.exec(expression)) !== null) {
            const tableOrAlias = match[1].trim();
            const columnName = match[2].trim();
            
            // Resolve table alias to actual table name
            const actualTableName = tableAliases[tableOrAlias.toLowerCase()] || tableOrAlias;
            
            if (!tableColumnMap[actualTableName]) {
                tableColumnMap[actualTableName] = [];
            }
            
            if (!tableColumnMap[actualTableName].includes(columnName)) {
                tableColumnMap[actualTableName].push(columnName);
            }
        }
    }

    // Helper method to extract columns from WHERE, GROUP BY, ORDER BY clauses
    extractColumnsFromClauses(sql, tableAliases, tableColumnMap) {
        // Extract from WHERE clause
        const whereMatch = sql.match(/WHERE\s+(.*?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+HAVING|\s+LIMIT|\s*;|\s*$)/si);
        if (whereMatch) {
            this.extractTableColumnReferences(whereMatch[1], tableAliases, tableColumnMap);
        }
        
        // Extract from GROUP BY clause
        const groupByMatch = sql.match(/GROUP\s+BY\s+(.*?)(?:\s+HAVING|\s+ORDER\s+BY|\s+LIMIT|\s*;|\s*$)/si);
        if (groupByMatch) {
            this.extractTableColumnReferences(groupByMatch[1], tableAliases, tableColumnMap);
        }
        
        // Extract from ORDER BY clause
        const orderByMatch = sql.match(/ORDER\s+BY\s+(.*?)(?:\s+LIMIT|\s*;|\s*$)/si);
        if (orderByMatch) {
            this.extractTableColumnReferences(orderByMatch[1], tableAliases, tableColumnMap);
        }
        
        // Extract from HAVING clause
        const havingMatch = sql.match(/HAVING\s+(.*?)(?:\s+ORDER\s+BY|\s+LIMIT|\s*;|\s*$)/si);
        if (havingMatch) {
            this.extractTableColumnReferences(havingMatch[1], tableAliases, tableColumnMap);
        }
        
        // Extract from JOIN conditions
        const joinRegex = /(?:INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|JOIN)\s+[a-zA-Z_][a-zA-Z0-9_]*(?:\s+[a-zA-Z_][a-zA-Z0-9_]*)?\s+ON\s+(.*?)(?=\s+(?:INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|JOIN)|WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|;|$)/gi;
        let joinMatch;
        
        while ((joinMatch = joinRegex.exec(sql)) !== null) {
            this.extractTableColumnReferences(joinMatch[1], tableAliases, tableColumnMap);
        }
    }

    // Helper method to extract table aliases from SQL
    extractTableAliases(sql) {
        const aliases = {};
        
        // Extract FROM clause table and alias
        const fromMatch = sql.match(/FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\s*(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)?/i);
        if (fromMatch) {
            const tableName = fromMatch[1].split('.').pop(); // Remove schema if present
            const alias = fromMatch[2] || tableName;
            aliases[alias.toLowerCase()] = tableName;
        }
        
        // Extract JOIN clauses with aliases
        const joinRegex = /(?:INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\s*(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)?/gi;
        let joinMatch;
        
        while ((joinMatch = joinRegex.exec(sql)) !== null) {
            const tableName = joinMatch[1].split('.').pop(); // Remove schema if present
            const alias = joinMatch[2] || tableName;
            aliases[alias.toLowerCase()] = tableName;
        }
        
        return aliases;
    }

    // Helper method to split SELECT clause (reuse existing method)
    splitSelectClause(selectClause) {
        const parts = [];
        let current = '';
        let parenCount = 0;
        
        for (let i = 0; i < selectClause.length; i++) {
            const char = selectClause[i];
            
            if (char === '(') {
                parenCount++;
            } else if (char === ')') {
                parenCount--;
            } else if (char === ',' && parenCount === 0) {
                parts.push(current.trim());
                current = '';
                continue;
            }
            
            current += char;
        }
        
        if (current.trim()) {
            parts.push(current.trim());
        }
        
        return parts;
    }
    
    extractTablesFromSQL(sql) {
        const tableRegex = /(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)/gi;
        const matches = sql.match(tableRegex) || [];
        const tables = matches.map(match => 
            match.replace(/(?:FROM|JOIN)\s+/i, '').trim()
        );
        return [...new Set(tables)]; // Remove duplicates
    }
    
    extractColumnsFromSQL(sql) {
        const columnRegex = /SELECT\s+(.*?)\s+FROM/si;
        const match = sql.match(columnRegex);
        if (!match) return [];
        
        const selectClause = match[1];
        const columns = selectClause.split(',').map(col => {
            // Remove functions, aliases, etc. - just get basic column names
            const cleanCol = col.trim().replace(/.*\s+AS\s+/i, '').replace(/.*\./g, '');
            return cleanCol.replace(/[^a-zA-Z0-9_]/g, '').substring(0, 20);
        }).filter(col => col && col !== '*');
        
        return [...new Set(columns)];
    }
    
    // Fixed visualizeData method - only the relevant parts
    visualizeData(data) {
        const container = document.getElementById('graph-container');
        const placeholder = document.getElementById('graph-placeholder');
        
        // Remove placeholder
        if (placeholder) {
            placeholder.remove();
        }
        
        // Clear previous visualization
        container.innerHTML = '';
        
        // Validate data before proceeding
        if (!data.nodes || data.nodes.length === 0) {
            container.innerHTML = '<div class="error-message">No data to visualize. Please check your SQL query.</div>';
            return;
        }
        
        // IMPORTANT: Store original edge data before D3 modifies it
        const originalEdges = data.edges ? data.edges.map(edge => ({
            sourceId: edge.source,
            targetId: edge.target,
            type: edge.type
        })) : [];
        
        // Store original edges in the data object for later use
        data.originalEdges = originalEdges;
        
        // Create SVG
        const svg = d3.select('#graph-container')
            .append('svg')
            .attr('id', 'graph-svg')
            .attr('width', '100%')
            .attr('height', '100%');
        
        const width = container.clientWidth || 800;
        const height = Math.max(500, container.clientHeight || 600);
        
        svg.attr('viewBox', `0 0 ${width} ${height}`);
        
        // Define arrow markers
        const defs = svg.append('defs');
        defs.append('marker')
            .attr('id', 'arrowhead')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 25) // Increased to account for circle radius
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', '#666');
        
        // Create links data - ensure edges have valid source/target
        const validEdges = (data.edges || []).filter(edge => {
            const sourceExists = data.nodes.some(n => n.id === edge.source);
            const targetExists = data.nodes.some(n => n.id === edge.target);
            return sourceExists && targetExists;
        });
        
        // Helper function to calculate edge endpoints
        const getEdgeEndpoints = (source, target) => {
            const dx = target.x - source.x;
            const dy = target.y - source.y;
            const distance = Math.sqrt(dx * dx + dy * dy);
            
            if (distance === 0) return { x1: source.x, y1: source.y, x2: target.x, y2: target.y };
            
            const sourceRadius = source.type === 'query' ? 15 : 12;
            const targetRadius = target.type === 'query' ? 15 : 12;
            
            const unitX = dx / distance;
            const unitY = dy / distance;
            
            return {
                x1: source.x + unitX * sourceRadius,
                y1: source.y + unitY * sourceRadius,
                x2: target.x - unitX * targetRadius,
                y2: target.y - unitY * targetRadius
            };
        };
        
        // Create force simulation
        this.simulation = d3.forceSimulation(data.nodes)
            .force('link', d3.forceLink(validEdges).id(d => d.id).distance(100))
            .force('charge', d3.forceManyBody().strength(-300))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(35));
        
        // Create links
        const link = svg.append('g')
            .attr('class', 'links')
            .selectAll('line')
            .data(validEdges)
            .join('line')
            .attr('class', d => `link ${d.type}`)
            .attr('stroke', '#666')
            .attr('stroke-width', 2)
            .attr('marker-end', 'url(#arrowhead)');
        
        // Create nodes
        const node = svg.append('g')
            .attr('class', 'nodes')
            .selectAll('g')
            .data(data.nodes)
            .join('g')
            .attr('class', d => `node ${d.type}`)
            .call(this.drag(this.simulation));
        
        // Add circles to nodes
        node.append('circle')
            .attr('r', d => d.type === 'query' ? 15 : 12)
            .attr('fill', d => this.getNodeColor(d.type))
            .attr('stroke', '#fff')
            .attr('stroke-width', 2);
        
        // Add labels to nodes
        node.append('text')
            .attr('dy', 25)
            .attr('text-anchor', 'middle')
            .text(d => this.truncateText(d.name, 15))
            .style('font-size', '11px')
            .style('fill', '#333')
            .style('font-family', 'Arial, sans-serif');
        
        // Add tooltips
        node.append('title')
            .text(d => {
                const parts = [d.type.toUpperCase(), d.name];
                if (d.schema) parts.push(`Schema: ${d.schema}`);
                if (d.table) parts.push(`Table: ${d.table}`);
                return parts.join('\n');
            });
        
        // Update positions on simulation tick
        this.simulation.on('tick', () => {
            link.each(function(d) {
                const endpoints = getEdgeEndpoints(d.source, d.target);
                d3.select(this)
                    .attr('x1', endpoints.x1)
                    .attr('y1', endpoints.y1)
                    .attr('x2', endpoints.x2)
                    .attr('y2', endpoints.y2);
            });
            
            node.attr('transform', d => `translate(${d.x || 0},${d.y || 0})`);
        });
        
        // Add legend
        this.addLegend(container);
        
        // Add zoom behavior
        const zoom = d3.zoom()
            .scaleExtent([0.1, 4])
            .on('zoom', (event) => {
                svg.selectAll('g.nodes, g.links')
                    .attr('transform', event.transform);
            });
        
        svg.call(zoom);
    }
    
    addLegend(container) {
        const legend = d3.select(container)
            .append('div')
            .attr('class', 'legend')
            .style('position', 'absolute')
            .style('top', '10px')
            .style('right', '10px')
            .style('background', 'rgba(255,255,255,0.9)')
            .style('padding', '10px')
            .style('border-radius', '5px')
            .style('border', '1px solid #ccc');
        
        const legendData = [
            { type: 'table', color: '#4CAF50', label: 'Table' },
            { type: 'column', color: '#2196F3', label: 'Column' },
            { type: 'query', color: '#FF9800', label: 'Query' }
        ];
        
        const legendItems = legend.selectAll('.legend-item')
            .data(legendData)
            .join('div')
            .attr('class', 'legend-item')
            .style('display', 'flex')
            .style('align-items', 'center')
            .style('margin', '5px 0');
        
        legendItems.append('div')
            .attr('class', 'legend-color')
            .style('width', '12px')
            .style('height', '12px')
            .style('background-color', d => d.color)
            .style('margin-right', '8px')
            .style('border-radius', '50%');
        
        legendItems.append('span')
            .text(d => d.label)
            .style('font-size', '12px');
    }
    
    getNodeColor(type) {
        const colors = {
            'table': '#4CAF50',
            'column': '#2196F3',
            'query': '#FF9800',
            'unknown': '#666'
        };
        return colors[type] || '#666';
    }
    
    truncateText(text, maxLength) {
        if (!text) return 'Unnamed';
        return text.length > maxLength ? text.substring(0, maxLength) + '...' : text;
    }
    
    drag(simulation) {
        function dragstarted(event, d) {
            if (!event.active) simulation.alphaTarget(0.3).restart();
            d.fx = d.x;
            d.fy = d.y;
        }
        
        function dragged(event, d) {
            d.fx = event.x;
            d.fy = event.y;
        }
        
        function dragended(event, d) {
            if (!event.active) simulation.alphaTarget(0);
            d.fx = null;
            d.fy = null;
        }
        
        return d3.drag()
            .on('start', dragstarted)
            .on('drag', dragged)
            .on('end', dragended);
    }
    
    updateDetails(data) {
        const detailsContainer = document.getElementById('details-content');
        
        if (!data.nodes || data.nodes.length === 0) {
            detailsContainer.innerHTML = '<p>No data available to display details.</p>';
            return;
        }
        
        const tables = data.nodes.filter(n => n.type === 'table');
        const columns = data.nodes.filter(n => n.type === 'column');
        const queries = data.nodes.filter(n => n.type === 'query');
        
        // Create a node lookup map for quick access
        const nodeMap = {};
        data.nodes.forEach(node => {
            nodeMap[node.id] = node;
        });
        
        // Helper function to get node name by ID
        const getNodeName = (nodeId) => {
            const node = nodeMap[nodeId];
            return node ? node.name : nodeId;
        };
        
        // Helper function to get relationship display text
        const getRelationshipText = (type) => {
            const relationshipMap = {
                'provides': 'provides',
                'flows_to': 'flows to',
                'lineage': 'has lineage from',
                'sources': 'sources',
                'constrains': 'constrains',
                'modifies': 'modifies',
                'feeds': 'feeds into',
                'uses': 'uses',
                'depends_on': 'depends on'
            };
            return relationshipMap[type] || type;
        };
        
        // Use original edges if available, otherwise fall back to current edges
        const edgesToDisplay = data.originalEdges || data.edges || [];
        
        detailsContainer.innerHTML = `
            <div class="detail-section">
                <h3>Summary</h3>
                <ul class="detail-list">
                    <li><strong>Total Tables:</strong> ${tables.length}</li>
                    <li><strong>Total Columns:</strong> ${columns.length}</li>
                    <li><strong>Total Queries:</strong> ${queries.length}</li>
                    <li><strong>Total Relationships:</strong> ${edgesToDisplay.length}</li>
                </ul>
            </div>
            
            ${tables.length > 0 ? `
            <div class="detail-section">
                <h3>Tables</h3>
                <div>
                    ${tables.map(t => `
                        <span class="table-info" style="display: inline-block; margin: 2px 5px; padding: 2px 8px; background: #e8f5e8; border-radius: 3px; font-size: 12px;">
                            ${t.schema ? `${t.schema}.` : ''}${t.name}
                        </span>
                    `).join('')}
                </div>
            </div>
            ` : ''}
            
            ${columns.length > 0 ? `
            <div class="detail-section">
                <h3>Columns</h3>
                <div>
                    ${columns.map(c => `
                        <span class="column-info" style="display: inline-block; margin: 2px 5px; padding: 2px 8px; background: #e3f2fd; border-radius: 3px; font-size: 12px;">
                            ${c.schema ? `${c.schema}.` : ''}${c.name}
                        </span>
                    `).join('')}
                </div>
            </div>
            ` : ''}
            
            ${edgesToDisplay.length > 0 ? `
            <div class="detail-section">
                <h3>Relationships</h3>
                <ul class="detail-list">
                    ${edgesToDisplay.map(e => {
                        // Handle both original edges format and current edges format
                        const sourceId = e.sourceId || e.source;
                        const targetId = e.targetId || e.target;
                        const sourceName = getNodeName(sourceId);
                        const targetName = getNodeName(targetId);
                        
                        return `
                            <li>
                                <strong>[${sourceName}]</strong> 
                                <em style="color: #666; font-style: italic;">--${getRelationshipText(e.type)}--></em> 
                                <strong>[${targetName}]</strong>
                            </li>
                        `;
                    }).join('')}
                </ul>
            </div>
            ` : ''}
        `;
    }
    
    updateJSON(data) {
        const jsonOutput = document.getElementById('json-output');
        if (jsonOutput) {
            jsonOutput.textContent = JSON.stringify(data, null, 2);
        }
    }

    isValidColumnName(name) {
        if (!name || typeof name !== 'string') return false;
        
        // Remove any whitespace
        name = name.trim();
        
        // Check if it's a valid column name (starts with letter or underscore)
        const isValidFormat = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name);
        
        // Check if it's not a SQL keyword
        const isNotKeyword = !this.isSQLKeyword(name);
        
        return isValidFormat && isNotKeyword;
    }

    // Also add this helper method to the SQLLineageVisualizer class
    isSQLKeyword(word) {
        const keywords = [
            'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL',
            'ON', 'AND', 'OR', 'NOT', 'NULL', 'TRUE', 'FALSE', 'COUNT', 'SUM',
            'AVG', 'MIN', 'MAX', 'GROUP', 'BY', 'ORDER', 'HAVING', 'LIMIT',
            'DISTINCT', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'LIKE',
            'IN', 'BETWEEN', 'EXISTS', 'ALL', 'ANY', 'SOME', 'UNION', 'INTERSECT',
            'EXCEPT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP',
            'TABLE', 'VIEW', 'INDEX', 'DATABASE', 'SCHEMA', 'PRIMARY', 'KEY',
            'FOREIGN', 'REFERENCES', 'CONSTRAINT', 'UNIQUE', 'CHECK', 'DEFAULT',
            'AUTO_INCREMENT', 'IDENTITY', 'TIMESTAMP', 'DATETIME', 'DATE', 'TIME',
            'VARCHAR', 'CHAR', 'TEXT', 'INT', 'INTEGER', 'BIGINT', 'SMALLINT',
            'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'BOOLEAN', 'BOOL'
        ];
        
        return keywords.includes(word.toUpperCase());
    }
    
    loadExample() {
        const exampleSQL = `-- E-commerce Analytics Query
SELECT 
    u.user_id,
    u.name,
    u.email,
    COUNT(o.order_id) as total_orders,
    SUM(oi.quantity * p.price) as total_spent,
    AVG(oi.quantity * p.price) as avg_order_value
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
LEFT JOIN products p ON oi.product_id = p.product_id
WHERE o.status = 'completed'
    AND o.created_at >= '2024-01-01'
GROUP BY u.user_id, u.name, u.email
HAVING COUNT(o.order_id) > 0
ORDER BY total_spent DESC
LIMIT 100;`;
        
        const sqlInput = document.getElementById('sql-input');
        if (sqlInput) {
            sqlInput.value = exampleSQL;
        }
    }
    
    clearAll() {
        const sqlInput = document.getElementById('sql-input');
        const graphContainer = document.getElementById('graph-container');
        const detailsContent = document.getElementById('details-content');
        const jsonOutput = document.getElementById('json-output');
        
        if (sqlInput) {
            sqlInput.value = '';
        }
        
        if (graphContainer) {
            graphContainer.innerHTML = `
                <div id="graph-placeholder" style="display: flex; align-items: center; justify-content: center; height: 100%; color: #666;">
                    <p>Enter a SQL query above and click "Parse & Visualize" to see the data lineage graph.</p>
                </div>
            `;
        }
        
        if (detailsContent) {
            detailsContent.innerHTML = '<p>Query details will appear here after parsing.</p>';
        }
        
        if (jsonOutput) {
            jsonOutput.textContent = 'JSON output will appear here after parsing.';
        }
        
        this.currentData = null;
        
        // Stop simulation if running
        if (this.simulation) {
            this.simulation.stop();
        }
    }
    
    showLoading(show) {
        const loading = document.getElementById('loading');
        if (loading) {
            loading.classList.toggle('hidden', !show);
        }
    }
    
    showError(message) {
        const errorDiv = document.getElementById('error-message');
        if (errorDiv) {
            errorDiv.textContent = message;
            errorDiv.classList.remove('hidden');
            
            setTimeout(() => {
                errorDiv.classList.add('hidden');
            }, 5000);
        } else {
            // Fallback to console and alert if error div doesn't exist
            console.error('Error:', message);
            alert(`Error: ${message}`);
        }
    }
}

// Initialize the visualizer when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    try {
        new SQLLineageVisualizer();
    } catch (error) {
        console.error('Failed to initialize SQL Lineage Visualizer:', error);
    }
});