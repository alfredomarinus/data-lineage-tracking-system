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
            // This parser extracts original columns, aliases are handled by visualizer
            this.columns = [...new Set([...this.columns, ...this.parseSelectClause(selectClause)])];
        }
        
        // Extract tables from FROM clause
        const fromMatch = sql.match(/FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)/i);
        if (fromMatch) {
            const tableName = fromMatch[1].split('.').pop(); // Remove schema if present
            if (!this.tables.includes(tableName)) {
                this.tables.push(tableName);
            }
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
        
        if (selectClause.trim() === '*') {
            return ['*'];
        }
        
        const parts = this.splitSelectClause(selectClause);
        
        for (let part of parts) {
            part = part.trim();
            if (!part) continue;
            
            // Get the expression before AS or any function for the actual column
            const originalColumn = this.extractOriginalColumnFromExpression(part);
            if (originalColumn && this.isValidColumnName(originalColumn)) {
                columns.push(originalColumn);
            }
        }
        
        return [...new Set(columns)];
    }

    extractOriginalColumnFromExpression(expr) {
        // Remove AS alias first
        let cleanExpr = expr.replace(/\s+AS\s+[a-zA-Z_][a-zA-Z0-9_]*/i, '');
        
        // Handle functions (e.g., COUNT(column), SUM(table.column))
        const funcMatch = cleanExpr.match(/[a-zA-Z_][a-zA-Z0-9_]*\s*\((.*?)\)/i);
        if (funcMatch && funcMatch[1]) {
            cleanExpr = funcMatch[1]; // Get content inside parentheses
        }

        // Handle table.column format
        if (cleanExpr.includes('.')) {
            const parts = cleanExpr.split('.');
            return parts[parts.length - 1].trim();
        }
        
        // Handle simple column name
        if (this.isValidColumnName(cleanExpr)) {
            return cleanExpr;
        }

        return null;
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
        
        const cleanExpr = expr.replace(/[a-zA-Z_][a-zA-Z0-9_]*\s*\(/g, '(');
        
        const columnRegex = /(?:^|[^a-zA-Z0-9_])([a-zA-Z_][a-zA-Z0-9_]*\.)?([a-zA-Z_][a-zA-Z0-9_]*)/g;
        let match;
        
        while ((match = columnRegex.exec(cleanExpr)) !== null) {
            const columnName = match[2];
            
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
    
    isValidColumnName(name) {
        return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name) && !this.isSQLKeyword(name);
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
}

class SQLLineageVisualizer {
    constructor() {
        this.apiUrl = '/api'; // Placeholder, not used in client-side parsing
        this.currentData = null;
        this.svg = null;
        this.simulation = null;
        this.lineageParser = new SQLLineageParser();
        
        this.init();
    }
    
    init() {
        this.setupEventListeners();
        this.setupTabs();
    }
    
    setupEventListeners() {
        document.getElementById('parse-btn').addEventListener('click', () => this.parseSQLAndVisualize());
        document.getElementById('clear-btn').addEventListener('click', () => this.clearAll());
        document.getElementById('example-btn').addEventListener('click', () => this.loadExample());
        
        document.getElementById('sql-input').addEventListener('keydown', (e) => {
            if (e.ctrlKey && e.key === 'Enter') {
                this.parseSQLAndVisualize();
            }
        });
    }
    
    setupTabs() {
        const tabButtons = document.querySelectorAll('.tab-btn');
        const tabPanes = document.querySelectorAll('.tab-pane');
        
        tabButtons.forEach(button => {
            button.addEventListener('click', () => {
                const tabId = button.dataset.tab;
                
                tabButtons.forEach(btn => btn.classList.remove('active'));
                button.classList.add('active');
                
                tabPanes.forEach(pane => pane.classList.remove('active'));
                document.getElementById(tabId).classList.add('active');
            });
        });
    }
    
    async parseSQLAndVisualize() {
        const sqlInput = document.getElementById('sql-input').value.trim();
        
        if (!sqlInput) {
            this.showError('Please enter a SQL query');
            return;
        }
        
        this.showLoading(true);
        
        try {
            const parsedResult = this.lineageParser.parseSQL(sqlInput);
            const data = this.generateVisualizationData(sqlInput, parsedResult);
            // Added the missing method here:
            const validatedData = this.validateAndNormalizeData(data); 
            this.currentData = validatedData;
            
            this.visualizeData(validatedData);
            this.updateDetails(validatedData);
            this.updateJSON(validatedData);
            
            const vizTab = document.querySelector('.tab-btn[data-tab="visualization"]');
            if (vizTab) {
                vizTab.click();
            }
            
        } catch (error) {
            console.error('Parse or Visualization error:', error);
            this.showError(error.message || 'An unknown error occurred during parsing or visualization.');
        } finally {
            this.showLoading(false);
        }
    }
    
    // Placeholder method to resolve "not a function" error
    validateAndNormalizeData(data) {
        // In a real application, this method would perform validation,
        // normalize data formats, handle edge cases, etc.
        // For now, it simply returns the data as is.
        return data;
    }

    generateVisualizationData(sqlQuery, parserResult) {
        const nodes = [];
        const edges = [];
        let nodeIdCounter = 0;

        const nodeMap = new Map(); // Maps node name (lowercase) to node ID for tables and main query
        const columnNodeIdMap = new Map(); // Maps 'table.column' (lowercase) to node ID
        const aliasNodeIdMap = new Map(); // Maps alias name (lowercase) to node ID

        // Add Main Query node
        const mainQueryNodeId = 'query_main';
        nodes.push({ id: mainQueryNodeId, name: 'Main Query', type: 'query' });
        nodeMap.set('main_query', mainQueryNodeId);

        // Extract table aliases first for resolving qualified names
        const tableAliases = this.extractTableAliases(sqlQuery);
        
        // Add Tables
        parserResult.tables.forEach(tableName => {
            const nodeId = `table_${nodeIdCounter++}`;
            nodes.push({ id: nodeId, name: tableName, type: 'table' });
            nodeMap.set(tableName.toLowerCase(), nodeId);
        });

        // Parse all table.column references from the entire query (including SELECT, WHERE, ON clauses)
        const tableColumnReferences = {}; // Populate this map with actual table -> [columns]
        const allQualifiedColumnNames = []; // Collect all qualified column names found
        this.extractTableColumnReferences(sqlQuery, tableAliases, tableColumnReferences, allQualifiedColumnNames);
        
        // Create nodes for table-qualified columns (e.g., table_a.id, table_b.id)
        allQualifiedColumnNames.forEach(qualifiedColumnName => {
            if (!columnNodeIdMap.has(qualifiedColumnName.toLowerCase())) {
                const parts = qualifiedColumnName.split('.');
                const tableName = parts[0];
                const columnName = parts[1];
                const nodeId = `col_${nodeIdCounter++}`;
                nodes.push({ 
                    id: nodeId, 
                    name: `${tableName} > ${columnName}`, // Display name
                    type: 'column', 
                    originalQualifiedName: qualifiedColumnName // Internal identifier
                });
                columnNodeIdMap.set(qualifiedColumnName.toLowerCase(), nodeId);
            }
        });

        // Process SELECT clause to identify aliases and output columns
        const selectClause = sqlQuery.match(/SELECT\s+(.*?)\s+FROM/si)?.[1];
        const selectClauseParts = selectClause ? this.lineageParser.splitSelectClause(selectClause) : [];

        // Store resolved original qualified column names to their aliases
        const resolvedColumnAliases = new Map(); // 'table.column' (lowercase) -> alias name (lowercase)
        const outputColumns = new Set(); // Final columns projected by the SELECT (includes aliases)
        const selectedOriginalQualifiedColumns = new Set(); // Original qualified columns that are part of SELECT statement (before aliasing)

        selectClauseParts.forEach(part => {
            const trimmedPart = part.trim();
            const asMatch = trimmedPart.match(/^(.*?)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)$/i);
            
            if (asMatch) {
                const originalExpr = asMatch[1].trim();
                const aliasName = asMatch[2].trim().toLowerCase();
                
                const originalRefs = [];
                this.extractTableColumnReferences(originalExpr, tableAliases, null, originalRefs);
                
                if (originalRefs.length > 0) {
                    const originalQualifiedName = originalRefs[0].toLowerCase();
                    resolvedColumnAliases.set(originalQualifiedName, aliasName);
                    selectedOriginalQualifiedColumns.add(originalQualifiedName);
                } else {
                    // For non-qualified columns in SELECT (e.g., 'SELECT simple_col AS alias')
                    // This is complex. For now, if it's not a qualified name, assume it's an abstract input to the alias.
                    // Or could try to infer from the single FROM table if available.
                    const inferredColumnName = this.lineageParser.extractOriginalColumnFromExpression(originalExpr);
                    if (inferredColumnName) {
                        // Create a simple column node if not already created as a qualified one.
                        // This might result in duplicate node names if a qualified and unqualified version exists.
                        // For lineage, qualified is preferred.
                        if (!columnNodeIdMap.has(inferredColumnName.toLowerCase())) {
                            const nodeId = `col_${nodeIdCounter++}`;
                            nodes.push({ id: nodeId, name: inferredColumnName, type: 'column', originalQualifiedName: inferredColumnName });
                            columnNodeIdMap.set(inferredColumnName.toLowerCase(), nodeId);
                        }
                        selectedOriginalQualifiedColumns.add(inferredColumnName.toLowerCase()); // Add simple name to selected list
                    }
                }
                outputColumns.add(aliasName); // The alias itself is an output column
            } else {
                // No AS alias, the output column is the expression itself
                const originalRefs = [];
                this.extractTableColumnReferences(trimmedPart, tableAliases, null, originalRefs);
                if (originalRefs.length > 0) {
                     const originalQualifiedName = originalRefs[0].toLowerCase();
                     selectedOriginalQualifiedColumns.add(originalQualifiedName);
                     outputColumns.add(originalQualifiedName); // The original qualified name is also an output
                } else {
                    // For non-qualified, non-aliased columns in SELECT (e.g., 'SELECT simple_col FROM table')
                    const inferredColumnName = this.lineageParser.extractOriginalColumnFromExpression(trimmedPart);
                     if (inferredColumnName) {
                        // If it's not already a qualified column node, add it as a simple column.
                        if (!columnNodeIdMap.has(inferredColumnName.toLowerCase())) {
                            const nodeId = `col_${nodeIdCounter++}`;
                            nodes.push({ id: nodeId, name: inferredColumnName, type: 'column', originalQualifiedName: inferredColumnName });
                            columnNodeIdMap.set(inferredColumnName.toLowerCase(), nodeId);
                        }
                        selectedOriginalQualifiedColumns.add(inferredColumnName.toLowerCase());
                        outputColumns.add(inferredColumnName.toLowerCase());
                     } else if (trimmedPart === '*') {
                         // Handle SELECT * explicitly for output
                         outputColumns.add('*');
                         if (!columnNodeIdMap.has('*')) { // Create '*' node if not exists
                             const nodeId = `col_${nodeIdCounter++}`;
                             nodes.push({ id: nodeId, name: '*', type: 'column', originalQualifiedName: '*' });
                             columnNodeIdMap.set('*', nodeId);
                         }
                     }
                }
            }
        });
        
        // Create nodes for aliases that are actual output columns
        outputColumns.forEach(outputName => {
            let isAliasNode = false;
            for (const [originalQualified, alias] of resolvedColumnAliases.entries()) {
                if (outputName === alias) {
                    isAliasNode = true;
                    break;
                }
            }

            if (isAliasNode && !aliasNodeIdMap.has(outputName)) {
                 const nodeId = `alias_${nodeIdCounter++}`;
                 nodes.push({ id: nodeId, name: outputName, type: 'column', isAlias: true });
                 aliasNodeIdMap.set(outputName, nodeId);
                 edges.push({ source: nodeId, target: mainQueryNodeId, type: 'flows_to' });
            } else if (!isAliasNode && columnNodeIdMap.has(outputName) && !selectedOriginalQualifiedColumns.has(outputName)) {
                // If it's a simple column name that made it to outputColumns but wasn't explicitly selected qualified
                // AND it wasn't already made an alias node, and is a valid column node.
                // This is a safety for implicitly selected simple columns.
                const qualifiedColNodeId = columnNodeIdMap.get(outputName);
                 if (qualifiedColNodeId) {
                     edges.push({ source: qualifiedColNodeId, target: mainQueryNodeId, type: 'flows_to' });
                 }
            } else if (outputName === '*' && columnNodeIdMap.has('*')) {
                edges.push({ source: columnNodeIdMap.get('*'), target: mainQueryNodeId, type: 'flows_to' });
            }
        });

        // Create relationships: Table --provides--> Qualified Column
        Object.entries(tableColumnReferences).forEach(([tableName, columns]) => {
            const tableNodeId = nodeMap.get(tableName.toLowerCase());
            if (tableNodeId) {
                columns.forEach(columnName => {
                    const qualifiedColumnName = `${tableName}.${columnName}`;
                    const qualifiedColNodeId = columnNodeIdMap.get(qualifiedColumnName.toLowerCase());
                    if (qualifiedColNodeId) {
                        edges.push({ source: tableNodeId, target: qualifiedColNodeId, type: 'provides' });
                    }
                });
            }
        });

        // Create relationships: Qualified Column -> Alias -> Main Query, or Qualified Column -> Main Query
        selectedOriginalQualifiedColumns.forEach(qualifiedColName => {
            const qualifiedColNodeId = columnNodeIdMap.get(qualifiedColName);
            if (qualifiedColNodeId) {
                const aliasName = resolvedColumnAliases.get(qualifiedColName);
                if (aliasName) {
                    const aliasColNodeId = aliasNodeIdMap.get(aliasName);
                    if (aliasColNodeId) {
                        edges.push({ source: qualifiedColNodeId, target: aliasColNodeId, type: 'flows_to' });
                        // Alias -> Main Query is already added when alias node is created
                    }
                } else {
                    // Direct flow to Main Query if not aliased and is a selected output column
                    // Ensure it's not already linked via another path (e.g., from an alias)
                    if (outputColumns.has(qualifiedColName) || outputColumns.has(qualifiedColName.split('.').pop())) {
                        const alreadyLinked = edges.some(e => e.source === qualifiedColNodeId && e.target === mainQueryNodeId && e.type === 'flows_to');
                        if (!alreadyLinked) {
                            edges.push({ source: qualifiedColNodeId, target: mainQueryNodeId, type: 'flows_to' });
                        }
                    }
                }
            }
        });

        // Handle JOIN conditions for relationships between columns
        const joinConditions = this.extractJoinConditions(sqlQuery, tableAliases);
        joinConditions.forEach(condition => {
            if (condition.leftColumnQualified && condition.rightColumnQualified) {
                const leftNodeId = columnNodeIdMap.get(condition.leftColumnQualified.toLowerCase());
                const rightNodeId = columnNodeIdMap.get(condition.rightColumnQualified.toLowerCase());
                if (leftNodeId && rightNodeId) {
                    edges.push({ source: leftNodeId, target: rightNodeId, type: 'uses', label: 'on_join' }); // Add a label for visualization
                    edges.push({ source: rightNodeId, target: leftNodeId, type: 'uses', label: 'on_join' }); // Bidirectional for join
                }
            }
        });

        // Handle WHERE clause constraints
        const whereClause = sqlQuery.match(/WHERE\s+(.+?)(?:\s+GROUP|\s+ORDER|\s+HAVING|\s*;|\s*$)/si)?.[1];
        if (whereClause) {
            const constrainedQualifiedColumns = [];
            this.extractTableColumnReferences(whereClause, tableAliases, null, constrainedQualifiedColumns);
            constrainedQualifiedColumns.forEach(qualifiedColName => {
                const colNodeId = columnNodeIdMap.get(qualifiedColName.toLowerCase());
                if (colNodeId) {
                    edges.push({ source: colNodeId, target: mainQueryNodeId, type: 'constrains' });
                }
            });
        }

        // Handle UPDATE queries
        const sqlUpper = sqlQuery.toUpperCase();
        if (sqlUpper.includes('UPDATE')) {
            const updateMatch = sqlQuery.match(/UPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)/i);
            if (updateMatch) {
                const updateTable = updateMatch[1];
                const updateTableNodeId = nodeMap.get(updateTable.toLowerCase());
                if (updateTableNodeId) {
                    edges.push({ source: mainQueryNodeId, target: updateTableNodeId, type: 'modifies' });
                }
            }
        }
        
        // Handle INSERT queries
        if (sqlUpper.includes('INSERT INTO')) {
            const insertMatch = sqlQuery.match(/INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)/i);
            if (insertMatch) {
                const insertTable = insertMatch[1];
                const insertTableNodeId = nodeMap.get(insertTable.toLowerCase());
                if (insertTableNodeId) {
                    edges.push({ source: mainQueryNodeId, target: insertTableNodeId, type: 'modifies' });
                }
            }
        }

        return { nodes, edges };
    }


    // Modified helper method to extract table.column references from an expression
    // It can populate a tableColumnMap (for general references) and/or a qualifiedNamesArray (for direct collection)
    extractTableColumnReferences(expression, tableAliases, tableColumnMap = null, qualifiedNamesArray = []) {
        const tableColumnRegex = /([a-zA-Z_][a-zA-Z0-9_]*)\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]*)/g;
        let match;
        
        while ((match = tableColumnRegex.exec(expression)) !== null) {
            const tableOrAlias = match[1].trim();
            const columnName = match[2].trim();
            
            const actualTableName = tableAliases[tableOrAlias.toLowerCase()] || tableOrAlias;
            
            if (tableColumnMap) {
                if (!tableColumnMap[actualTableName]) {
                    tableColumnMap[actualTableName] = [];
                }
                if (!tableColumnMap[actualTableName].includes(columnName)) {
                    tableColumnMap[actualTableName].push(columnName);
                }
            }

            const qualifiedName = `${actualTableName}.${columnName}`;
            if (!qualifiedNamesArray.includes(qualifiedName)) {
                qualifiedNamesArray.push(qualifiedName);
            }
        }
    }

    // Helper method to extract columns from WHERE, GROUP BY, ORDER BY, HAVING clauses and JOIN ON conditions
    extractColumnsFromClauses(sql, tableAliases, tableColumnMap) {
        // This method is now implicitly covered by direct calls to extractTableColumnReferences in generateVisualizationData
        // and the comprehensive initial call to extractTableColumnReferences for allQualifiedColumnNames.
        // Keeping it for clarity if other parts relied on it directly.
        
        // WHERE clause
        const whereMatch = sql.match(/WHERE\s+(.*?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+HAVING|\s+LIMIT|\s*;|\s*$)/si);
        if (whereMatch) {
            this.extractTableColumnReferences(whereMatch[1], tableAliases, tableColumnMap);
        }
        
        // GROUP BY clause
        const groupByMatch = sql.match(/GROUP\s+BY\s+(.*?)(?:\s+HAVING|\s+ORDER\s+BY|\s+LIMIT|\s*;|\s*$)/si);
        if (groupByMatch) {
            this.extractTableColumnReferences(groupByMatch[1], tableAliases, tableColumnMap);
        }
        
        // ORDER BY clause
        const orderByMatch = sql.match(/ORDER\s+BY\s+(.*?)(?:\s+LIMIT|\s*;|\s*$)/si);
        if (orderByMatch) {
            this.extractTableColumnReferences(orderByMatch[1], tableAliases, tableColumnMap);
        }
        
        // HAVING clause
        const havingMatch = sql.match(/HAVING\s+(.*?)(?:\s+ORDER\s+BY|\s+LIMIT|\s*;|\s*$)/si);
        if (havingMatch) {
            this.extractTableColumnReferences(havingMatch[1], tableAliases, tableColumnMap);
        }
        
        // JOIN ON conditions are now handled by extractJoinConditions
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

    // New helper to parse join conditions specifically
    extractJoinConditions(sql, tableAliases) {
        const joinConditions = [];
        const joinRegex = /(?:INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|JOIN)\s+[a-zA-Z_][a-zA-Z0-9_]*(?:\s+AS\s+[a-zA-Z_][a-zA-Z0-9_]*)?\s+ON\s+(.*?)(?=\s+(?:INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|JOIN|WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|;|$))/gi;
        let match;
        
        while ((match = joinRegex.exec(sql)) !== null) {
            const onClause = match[1];
            const conditionRegex = /([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)/g;
            let condMatch;
            while ((condMatch = conditionRegex.exec(onClause)) !== null) {
                const leftExpr = condMatch[1].trim();
                const rightExpr = condMatch[2].trim();
                
                const leftParts = leftExpr.split('.');
                const leftTableOrAlias = leftParts[0];
                const leftColumnName = leftParts[1];
                const leftActualTable = tableAliases[leftTableOrAlias.toLowerCase()] || leftTableOrAlias;
                const leftColumnQualified = `${leftActualTable}.${leftColumnName}`;

                const rightParts = rightExpr.split('.');
                const rightTableOrAlias = rightParts[0];
                const rightColumnName = rightParts[1];
                const rightActualTable = tableAliases[rightTableOrAlias.toLowerCase()] || rightTableOrAlias;
                const rightColumnQualified = `${rightActualTable}.${rightColumnName}`;

                joinConditions.push({ leftColumnQualified, rightColumnQualified });
            }
        }
        return joinConditions;
    }
    
    visualizeData(data) {
        const container = document.getElementById('graph-container');
        const placeholder = document.getElementById('graph-placeholder');
        
        if (placeholder) {
            placeholder.remove();
        }
        
        container.innerHTML = '';
        
        if (!data.nodes || data.nodes.length === 0) {
            container.innerHTML = '<div class="error-message">No data to visualize. Please check your SQL query.</div>';
            return;
        }
        
        const originalEdges = data.edges ? data.edges.map(edge => ({
            sourceId: edge.source,
            targetId: edge.target,
            type: edge.type,
            label: edge.label // Preserve label for display
        })) : [];
        
        data.originalEdges = originalEdges;
        
        const svg = d3.select('#graph-container')
            .append('svg')
            .attr('id', 'graph-svg')
            .attr('width', '100%')
            .attr('height', '100%');
        
        const width = container.clientWidth || 800;
        const height = Math.max(500, container.clientHeight || 600);
        
        svg.attr('viewBox', `0 0 ${width} ${height}`);
        
        const defs = svg.append('defs');
        defs.append('marker')
            .attr('id', 'arrowhead')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 18) 
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', '#666');
        
        const validEdges = (data.edges || []).filter(edge => {
            const sourceExists = data.nodes.some(n => n.id === edge.source);
            const targetExists = data.nodes.some(n => n.id === edge.target);
            return sourceExists && targetExists;
        });
        
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
        
        this.simulation = d3.forceSimulation(data.nodes)
            .force('link', d3.forceLink(validEdges).id(d => d.id).distance(100))
            .force('charge', d3.forceManyBody().strength(-300))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(35));
        
        const link = svg.append('g')
            .attr('class', 'links')
            .selectAll('line')
            .data(validEdges)
            .join('line')
            .attr('class', d => `link ${d.type} ${d.label || ''}`) // Add label as class for styling
            .attr('stroke', '#666')
            .attr('stroke-width', 2)
            .attr('marker-end', 'url(#arrowhead)');
        
        const node = svg.append('g')
            .attr('class', 'nodes')
            .selectAll('g')
            .data(data.nodes)
            .join('g')
            .attr('class', d => `node ${d.type}`)
            .call(this.drag(this.simulation));
        
        node.append('circle')
            .attr('r', d => d.type === 'query' ? 15 : 12)
            .attr('fill', d => this.getNodeColor(d.type, d.isAlias))
            .attr('stroke', '#fff')
            .attr('stroke-width', 2);
        
        node.append('text')
            .attr('dy', 25)
            .attr('text-anchor', 'middle')
            .text(d => this.truncateText(d.name, 15))
            .style('font-size', '11px')
            .style('fill', '#333')
            .style('font-family', 'Arial, sans-serif');
        
        node.append('title')
            .text(d => {
                const parts = [d.type.toUpperCase(), d.name];
                if (d.schema) parts.push(`Schema: ${d.schema}`);
                if (d.table) parts.push(`Table: ${d.table}`);
                if (d.isAlias) parts.push('(Alias)');
                return parts.join('\n');
            });
        
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
        
        this.addLegend(container);
        
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
            .style('border', '1px solid #ccc')
            .style('z-index', 10); 
        
        const legendData = [
            { type: 'table', color: '#4CAF50', label: 'Table' },
            { type: 'column', color: '#2196F3', label: 'Column' },
            { type: 'column-alias', color: '#8E24AA', label: 'Column (Alias)' },
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
    
    getNodeColor(type, isAlias = false) {
        if (type === 'column' && isAlias) {
            return '#8E24AA'; 
        }
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
        const columns = data.nodes.filter(n => n.type === 'column' && !n.isAlias);
        const aliasedColumns = data.nodes.filter(n => n.type === 'column' && n.isAlias);
        const queries = data.nodes.filter(n => n.type === 'query');
        
        const nodeMap = {};
        data.nodes.forEach(node => {
            nodeMap[node.id] = node;
        });
        
        const getNodeName = (nodeId) => {
            const node = nodeMap[nodeId];
            return node ? node.name + (node.isAlias ? ' (Alias)' : '') : nodeId;
        };
        
        const getRelationshipText = (type, label = null) => {
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
            return label || relationshipMap[type] || type;
        };
        
        const edgesToDisplay = data.originalEdges || data.edges || [];
        
        detailsContainer.innerHTML = `
            <div class="detail-section">
                <h3>Summary</h3>
                <ul class="detail-list">
                    <li><strong>Total Tables:</strong> ${tables.length}</li>
                    <li><strong>Total Columns:</strong> ${columns.length}</li>
                    <li><strong>Total Aliased Columns:</strong> ${aliasedColumns.length}</li>
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
            
            ${columns.length > 0 || aliasedColumns.length > 0 ? `
            <div class="detail-section">
                <h3>Columns</h3>
                <div>
                    ${columns.map(c => `
                        <span class="column-info" style="display: inline-block; margin: 2px 5px; padding: 2px 8px; background: #e3f2fd; border-radius: 3px; font-size: 12px;">
                            ${c.name}
                        </span>
                    `).join('')}
                    ${aliasedColumns.map(c => `
                        <span class="column-info" style="display: inline-block; margin: 2px 5px; padding: 2px 8px; background: #f3e5f5; border-radius: 3px; font-size: 12px;">
                            ${c.name} (Alias)
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
                        const sourceId = e.sourceId || e.source;
                        const targetId = e.targetId || e.target;
                        const sourceName = getNodeName(sourceId);
                        const targetName = getNodeName(targetId);
                        
                        return `
                            <li>
                                <strong>[${sourceName}]</strong> 
                                <em style="color: #666; font-style: italic;">--${getRelationshipText(e.type, e.label)}--></em> 
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
    
    loadExample() {
        const exampleSQL = `-- E-commerce Analytics Query with Aliases
SELECT 
    u.user_id AS user_identifier,
    u.name,
    u.email,
    COUNT(o.order_id) as total_orders_count,
    SUM(oi.quantity * p.price) as total_amount_spent,
    AVG(oi.quantity * p.price) as average_order_value_usd
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
LEFT JOIN products p ON oi.product_id = p.product_id
WHERE o.status = 'completed'
    AND o.created_at >= '2024-01-01'
GROUP BY u.user_id, u.name, u.email
HAVING COUNT(o.order_id) > 0
ORDER BY total_amount_spent DESC
LIMIT 100;

-- Simple Query with Alias and Join for testing
SELECT a.id AS new_id, a.name, b.value AS b_value
FROM table_a a
LEFT JOIN table_b b ON a.id = b.id AND a.status = 'active'
WHERE a.created_at > '2023-01-01';

-- Another test case with non-qualified selected columns
SELECT id, name FROM another_table;

`;
        
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
            console.error('Error:', message);
            alert(`Error: ${message}`);
        }
    }
}

document.addEventListener('DOMContentLoaded', () => {
    try {
        new SQLLineageVisualizer();
    } catch (error) {
        console.error('Failed to initialize SQL Lineage Visualizer:', error);
    }
});