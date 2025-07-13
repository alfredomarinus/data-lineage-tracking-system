class SQLLineageParser {
    constructor() {
        this.sqlKeywords = new Set([
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
        ]);
    }
    
    parseSQL(sqlQuery) {
        const cleanQuery = sqlQuery.trim().replace(/\s+/g, ' ');
        const tables = this.extractTables(cleanQuery);
        const columns = this.extractColumns(cleanQuery);
        const relationships = this.generateRelationships(tables, columns);
        
        return {
            query: cleanQuery,
            relationships,
            tables,
            columns
        };
    }
    
    extractTables(sql) {
        const tables = [];
        
        // FROM clause
        const fromMatch = sql.match(/FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)/i);
        if (fromMatch) {
            tables.push(this.getTableName(fromMatch[1]));
        }
        
        // JOIN clauses
        const joinRegex = /(?:INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)/gi;
        let joinMatch;
        while ((joinMatch = joinRegex.exec(sql)) !== null) {
            const tableName = this.getTableName(joinMatch[1]);
            if (!tables.includes(tableName)) {
                tables.push(tableName);
            }
        }
        
        return tables;
    }
    
    extractColumns(sql) {
        const columns = new Set();
        
        // SELECT clause
        const selectMatch = sql.match(/SELECT\s+(.*?)\s+FROM/si);
        if (selectMatch) {
            this.parseSelectClause(selectMatch[1]).forEach(col => columns.add(col));
        }
        
        // WHERE, ORDER BY, GROUP BY clauses
        ['WHERE', 'ORDER\\s+BY', 'GROUP\\s+BY'].forEach(clause => {
            const regex = new RegExp(`${clause}\\s+(.*?)(?:\\s+(?:GROUP\\s+BY|ORDER\\s+BY|HAVING|LIMIT)|\\s*;|\\s*$)`, 'si');
            const match = sql.match(regex);
            if (match) {
                this.extractColumnsFromExpression(match[1]).forEach(col => columns.add(col));
            }
        });
        
        return Array.from(columns);
    }
    
    parseSelectClause(selectClause) {
        if (selectClause.trim() === '*') return ['*'];
        
        const columns = [];
        const parts = this.splitByComma(selectClause);
        
        for (let part of parts) {
            part = part.trim();
            if (!part) continue;
            
            // Handle AS aliases
            const asMatch = part.match(/^(.*?)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)/i);
            if (asMatch) {
                part = asMatch[1].trim();
            }
            
            // Extract columns from expression
            columns.push(...this.extractColumnsFromExpression(part));
        }
        
        return [...new Set(columns)];
    }
    
    splitByComma(text) {
        const parts = [];
        let current = '';
        let parenCount = 0;
        
        for (const char of text) {
            if (char === '(') parenCount++;
            else if (char === ')') parenCount--;
            else if (char === ',' && parenCount === 0) {
                parts.push(current.trim());
                current = '';
                continue;
            }
            current += char;
        }
        
        if (current.trim()) parts.push(current.trim());
        return parts;
    }
    
    extractColumnsFromExpression(expr) {
        const columns = [];
        const columnRegex = /(?:^|[^a-zA-Z0-9_])([a-zA-Z_][a-zA-Z0-9_]*\.)?([a-zA-Z_][a-zA-Z0-9_]*)/g;
        let match;
        
        while ((match = columnRegex.exec(expr)) !== null) {
            const columnName = match[2];
            if (this.isValidColumnName(columnName)) {
                columns.push(columnName);
            }
        }
        
        return columns;
    }
    
    generateRelationships(tables, columns) {
        const relationships = [];
        
        for (const table of tables) {
            for (const column of columns) {
                if (column !== '*') {
                    relationships.push({
                        source: table,
                        target: column,
                        type: 'provides'
                    });
                }
            }
        }
        
        return relationships;
    }
    
    getTableName(fullName) {
        return fullName.split('.').pop(); // Remove schema if present
    }
    
    isValidColumnName(name) {
        return name && 
               typeof name === 'string' && 
               /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name.trim()) && 
               !this.sqlKeywords.has(name.toUpperCase());
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
        this.simulation = null;
        
        // SQL regex patterns - consolidated
        this.patterns = {
            select: /SELECT\s+(.*?)\s+FROM/si,
            from: /FROM\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)?)\s*(?:AS\s+)?([a-zA-Z_]\w*)?/i,
            join: /(?:(?:INNER|LEFT|RIGHT|FULL)\s+)?JOIN\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)?)\s*(?:AS\s+)?([a-zA-Z_]\w*)?/gi,
            tableColumn: /([a-zA-Z_]\w*)\s*\.\s*([a-zA-Z_]\w*)/g,
            alias: /^(.*?)\s+AS\s+([a-zA-Z_]\w*)/i,
            clauses: /(?:WHERE|GROUP\s+BY|ORDER\s+BY|HAVING)\s+(.*?)(?=\s+(?:GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|;|$))/gi
        };
        
        this.sqlKeywords = new Set([
            'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL',
            'ON', 'AND', 'OR', 'NOT', 'NULL', 'TRUE', 'FALSE', 'COUNT', 'SUM',
            'AVG', 'MIN', 'MAX', 'GROUP', 'BY', 'ORDER', 'HAVING', 'LIMIT',
            'DISTINCT', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'
        ]);
        
        this.nodeColors = {
            table: '#4CAF50',
            column: '#2196F3', 
            query: '#FF9800',
            unknown: '#666'
        };
        
        this.init();
    }
    
    init() {
        this.setupEventListeners();
        this.setupTabs();
    }
    
    setupEventListeners() {
        const handlers = {
            'parse-btn': () => this.parseSQL(),
            'clear-btn': () => this.clearAll(),
            'example-btn': () => this.loadExample()
        };
        
        Object.entries(handlers).forEach(([id, handler]) => {
            document.getElementById(id)?.addEventListener('click', handler);
        });
        
        document.getElementById('sql-input')?.addEventListener('keydown', (e) => {
            if (e.ctrlKey && e.key === 'Enter') this.parseSQL();
        });
    }
    
    setupTabs() {
        document.querySelectorAll('.tab-btn').forEach(button => {
            button.addEventListener('click', () => {
                const tabId = button.dataset.tab;
                
                // Update active states
                document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
                document.querySelectorAll('.tab-pane').forEach(pane => pane.classList.remove('active'));
                
                button.classList.add('active');
                document.getElementById(tabId)?.classList.add('active');
            });
        });
    }
    
    async parseSQL() {
        const sqlInput = document.getElementById('sql-input')?.value.trim();
        if (!sqlInput) return this.showError('Please enter a SQL query');
        
        this.showLoading(true);
        
        try {
            // Try API first, fallback to local parsing
            let data;
            try {
                const response = await fetch(`${this.apiUrl}/parse`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ query: sqlInput })
                });
                
                if (response.ok) {
                    data = await response.json();
                } else {
                    throw new Error('API unavailable');
                }
            } catch {
                data = this.parseLocalSQL(sqlInput);
            }
            
            const normalizedData = this.normalizeData(data);
            this.currentData = normalizedData;
            
            this.updateVisualization(normalizedData);
            this.updateDetails(normalizedData);
            this.updateJSON(normalizedData);
            
            document.querySelector('.tab-btn[data-tab="visualization"]')?.click();
            
        } catch (error) {
            this.showError(error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    parseLocalSQL(sql) {
        const tables = this.extractTables(sql);
        const columns = this.extractColumns(sql);
        const nodes = [];
        const edges = [];
        
        // Create nodes
        tables.forEach((table, i) => {
            nodes.push({ id: `table_${i}`, name: table.name, type: 'table' });
        });
        
        columns.forEach((column, i) => {
            nodes.push({ id: `column_${i}`, name: column, type: 'column' });
        });
        
        if (nodes.length > 0) {
            nodes.push({ id: 'main_query', name: 'Main Query', type: 'query' });
        }
        
        // Create relationships
        this.createRelationships(edges, sql, tables, columns);
        
        return { nodes, edges };
    }
    
    extractTables(sql) {
        const tables = [];
        const tableMap = new Map();
        
        // FROM clause
        const fromMatch = sql.match(this.patterns.from);
        if (fromMatch) {
            const [, fullName, alias] = fromMatch;
            const name = fullName.split('.').pop();
            const tableInfo = { name, alias: alias || name };
            tables.push(tableInfo);
            tableMap.set(tableInfo.alias.toLowerCase(), name);
        }
        
        // JOIN clauses
        const joinMatches = [...sql.matchAll(this.patterns.join)];
        joinMatches.forEach(match => {
            const [, fullName, alias] = match;
            const name = fullName.split('.').pop();
            const tableInfo = { name, alias: alias || name };
            
            if (!tables.some(t => t.name === name)) {
                tables.push(tableInfo);
                tableMap.set(tableInfo.alias.toLowerCase(), name);
            }
        });
        
        return tables;
    }
    
    extractColumns(sql) {
        const selectMatch = sql.match(this.patterns.select);
        if (!selectMatch) return [];
        
        const columns = new Set();
        const parts = this.splitSelectClause(selectMatch[1]);
        
        parts.forEach(part => {
            const trimmed = part.trim();
            if (trimmed === '*') return;
            
            // Handle aliases
            const aliasMatch = trimmed.match(this.patterns.alias);
            if (aliasMatch) {
                columns.add(aliasMatch[2]);
                this.extractColumnNames(aliasMatch[1]).forEach(col => columns.add(col));
            } else {
                this.extractColumnNames(trimmed).forEach(col => columns.add(col));
            }
        });
        
        return Array.from(columns);
    }
    
    extractColumnNames(expression) {
        const columns = [];
        const matches = [...expression.matchAll(this.patterns.tableColumn)];
        
        matches.forEach(match => columns.push(match[2]));
        
        // Simple column names
        if (columns.length === 0 && /^[a-zA-Z_]\w*$/.test(expression.trim())) {
            const name = expression.trim();
            if (!this.sqlKeywords.has(name.toUpperCase())) {
                columns.push(name);
            }
        }
        
        return columns;
    }
    
    splitSelectClause(clause) {
        const parts = [];
        let current = '';
        let parenCount = 0;
        
        for (const char of clause) {
            if (char === '(') parenCount++;
            else if (char === ')') parenCount--;
            else if (char === ',' && parenCount === 0) {
                parts.push(current.trim());
                current = '';
                continue;
            }
            current += char;
        }
        
        if (current.trim()) parts.push(current.trim());
        return parts;
    }
    
    createRelationships(edges, sql, tables, columns) {
        // Table -> Column relationships
        tables.forEach((table, tableIndex) => {
            columns.forEach((column, columnIndex) => {
                edges.push({
                    source: `table_${tableIndex}`,
                    target: `column_${columnIndex}`,
                    type: 'provides'
                });
            });
        });
        
        // Column -> Query relationships
        columns.forEach((column, columnIndex) => {
            edges.push({
                source: `column_${columnIndex}`,
                target: 'main_query',
                type: 'flows_to'
            });
        });
        
        // Table -> Query relationships
        tables.forEach((table, tableIndex) => {
            edges.push({
                source: `table_${tableIndex}`,
                target: 'main_query',
                type: 'sources'
            });
        });
    }
    
    normalizeData(data) {
        const normalized = { nodes: [], edges: [] };
        
        // Normalize nodes
        if (data.nodes?.length) {
            normalized.nodes = data.nodes.map((node, i) => ({
                id: node.id || `node_${i}`,
                name: node.name || 'Unnamed',
                type: node.type || 'unknown'
            }));
        }
        
        // Normalize edges
        if (data.edges?.length) {
            normalized.edges = data.edges.map(edge => ({
                source: edge.source,
                target: edge.target,
                type: edge.type || 'uses'
            }));
        }
        
        return normalized;
    }
    
    updateVisualization(data) {
        const container = document.getElementById('graph-container');
        if (!container) return;
        
        container.innerHTML = '';
        
        if (!data.nodes?.length) {
            container.innerHTML = '<div class="error-message">No data to visualize</div>';
            return;
        }
        
        this.createD3Visualization(data, container);
    }
    
    createD3Visualization(data, container) {
        const width = container.clientWidth || 800;
        const height = Math.max(500, container.clientHeight || 600);
        
        const svg = d3.select(container)
            .append('svg')
            .attr('width', '100%')
            .attr('height', '100%')
            .attr('viewBox', `0 0 ${width} ${height}`);
        
        // Arrow markers
        svg.append('defs').append('marker')
            .attr('id', 'arrowhead')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 25)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', '#666');
        
        // Filter valid edges
        const validEdges = data.edges.filter(edge => 
            data.nodes.some(n => n.id === edge.source) && 
            data.nodes.some(n => n.id === edge.target)
        );
        
        // Create simulation
        this.simulation = d3.forceSimulation(data.nodes)
            .force('link', d3.forceLink(validEdges).id(d => d.id).distance(100))
            .force('charge', d3.forceManyBody().strength(-300))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(35));
        
        // Create links
        const link = svg.append('g')
            .selectAll('line')
            .data(validEdges)
            .join('line')
            .attr('stroke', '#666')
            .attr('stroke-width', 2)
            .attr('marker-end', 'url(#arrowhead)');
        
        // Create nodes
        const node = svg.append('g')
            .selectAll('g')
            .data(data.nodes)
            .join('g')
            .call(d3.drag()
                .on('start', (event, d) => {
                    if (!event.active) this.simulation.alphaTarget(0.3).restart();
                    d.fx = d.x; d.fy = d.y;
                })
                .on('drag', (event, d) => {
                    d.fx = event.x; d.fy = event.y;
                })
                .on('end', (event, d) => {
                    if (!event.active) this.simulation.alphaTarget(0);
                    d.fx = null; d.fy = null;
                }));
        
        // Node circles
        node.append('circle')
            .attr('r', d => d.type === 'query' ? 15 : 12)
            .attr('fill', d => this.nodeColors[d.type] || this.nodeColors.unknown)
            .attr('stroke', '#fff')
            .attr('stroke-width', 2);
        
        // Node labels
        node.append('text')
            .attr('dy', 25)
            .attr('text-anchor', 'middle')
            .style('font-size', '11px')
            .style('fill', '#333')
            .text(d => d.name.length > 15 ? d.name.substring(0, 15) + '...' : d.name);
        
        // Tooltips
        node.append('title').text(d => `${d.type.toUpperCase()}: ${d.name}`);
        
        // Update positions on tick
        this.simulation.on('tick', () => {
            link.attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);
            
            node.attr('transform', d => `translate(${d.x},${d.y})`);
        });
        
        // Add legend
        this.addLegend(container);
        
        // Add zoom
        svg.call(d3.zoom()
            .scaleExtent([0.1, 4])
            .on('zoom', (event) => {
                svg.selectAll('g').attr('transform', event.transform);
            }));
    }
    
    addLegend(container) {
        const legendData = [
            { type: 'table', color: this.nodeColors.table, label: 'Table' },
            { type: 'column', color: this.nodeColors.column, label: 'Column' },
            { type: 'query', color: this.nodeColors.query, label: 'Query' }
        ];
        
        const legend = d3.select(container)
            .append('div')
            .style('position', 'absolute')
            .style('top', '10px')
            .style('right', '10px')
            .style('background', 'rgba(255,255,255,0.9)')
            .style('padding', '10px')
            .style('border-radius', '5px')
            .style('border', '1px solid #ccc');
        
        legend.selectAll('div')
            .data(legendData)
            .join('div')
            .style('display', 'flex')
            .style('align-items', 'center')
            .style('margin', '5px 0')
            .html(d => `
                <div style="width: 12px; height: 12px; background: ${d.color}; margin-right: 8px; border-radius: 50%;"></div>
                <span style="font-size: 12px;">${d.label}</span>
            `);
    }
    
    updateDetails(data) {
        const container = document.getElementById('details-content');
        if (!container || !data.nodes?.length) {
            if (container) container.innerHTML = '<p>No data available</p>';
            return;
        }
        
        const nodesByType = {
            table: data.nodes.filter(n => n.type === 'table'),
            column: data.nodes.filter(n => n.type === 'column'),
            query: data.nodes.filter(n => n.type === 'query')
        };
        
        const summary = `
            <div class="detail-section">
                <h3>Summary</h3>
                <ul>
                    <li>Tables: ${nodesByType.table.length}</li>
                    <li>Columns: ${nodesByType.column.length}</li>
                    <li>Queries: ${nodesByType.query.length}</li>
                    <li>Relationships: ${data.edges?.length || 0}</li>
                </ul>
            </div>
        `;
        
        const createSection = (title, items, bgColor) => {
            if (!items.length) return '';
            const itemsHtml = items.map(item => 
                `<span style="display: inline-block; margin: 2px 5px; padding: 2px 8px; 
                 background: ${bgColor}; border-radius: 3px; font-size: 12px;">${item.name}</span>`
            ).join('');
            return `<div class="detail-section"><h3>${title}</h3><div>${itemsHtml}</div></div>`;
        };
        
        container.innerHTML = summary + 
            createSection('Tables', nodesByType.table, '#e8f5e8') +
            createSection('Columns', nodesByType.column, '#e3f2fd');
    }
    
    updateJSON(data) {
        const container = document.getElementById('json-output');
        if (container) {
            container.textContent = JSON.stringify(data, null, 2);
        }
    }
    
    loadExample() {
        const example = `SELECT 
    u.user_id,
    u.name,
    COUNT(o.order_id) as total_orders,
    SUM(oi.quantity * p.price) as total_spent
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
LEFT JOIN products p ON oi.product_id = p.product_id
WHERE o.status = 'completed'
GROUP BY u.user_id, u.name
ORDER BY total_spent DESC;`;
        
        const input = document.getElementById('sql-input');
        if (input) input.value = example;
    }
    
    clearAll() {
        const elements = ['sql-input', 'graph-container', 'details-content', 'json-output'];
        const placeholders = [
            '',
            '<div style="display: flex; align-items: center; justify-content: center; height: 100%; color: #666;"><p>Enter SQL query and click "Parse & Visualize"</p></div>',
            '<p>Query details will appear here</p>',
            'JSON output will appear here'
        ];
        
        elements.forEach((id, i) => {
            const el = document.getElementById(id);
            if (el) {
                if (id === 'sql-input') el.value = '';
                else el.innerHTML = placeholders[i];
            }
        });
        
        this.currentData = null;
        this.simulation?.stop();
    }
    
    showLoading(show) {
        const loading = document.getElementById('loading');
        if (loading) loading.classList.toggle('hidden', !show);
    }
    
    showError(message) {
        const error = document.getElementById('error-message');
        if (error) {
            error.textContent = message;
            error.classList.remove('hidden');
            setTimeout(() => error.classList.add('hidden'), 5000);
        } else {
            alert(`Error: ${message}`);
        }
    }
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    try {
        new SQLLineageVisualizer();
    } catch (error) {
        console.error('Failed to initialize:', error);
    }
});