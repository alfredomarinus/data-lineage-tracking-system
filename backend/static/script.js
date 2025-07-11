class SQLLineageVisualizer {
    constructor() {
        this.apiUrl = '/api';
        this.currentData = null;
        this.svg = null;
        this.simulation = null;
        
        this.init();
    }
    
    init() {
        this.setupEventListeners();
        this.setupTabs();
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
    
    createSampleDataFromSQL(normalized) {
        // Extract basic table/column info from SQL if API doesn't provide structured data
        const sqlInput = document.getElementById('sql-input').value;
        const tables = this.extractTablesFromSQL(sqlInput);
        const columns = this.extractColumnsFromSQL(sqlInput);
        
        tables.forEach((table, index) => {
            normalized.nodes.push({
                id: `table_${index}`,
                name: table,
                type: 'table'
            });
        });
        
        columns.forEach((column, index) => {
            normalized.nodes.push({
                id: `column_${index}`,
                name: column,
                type: 'column'
            });
        });
        
        // Create basic relationships
        if (tables.length > 0) {
            normalized.nodes.push({
                id: 'main_query',
                name: 'Main Query',
                type: 'query'
            });
            
            tables.forEach((table, index) => {
                normalized.edges.push({
                    source: `table_${index}`,
                    target: 'main_query',
                    type: 'feeds'
                });
            });
        }
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
            .attr('refX', 15)
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
            link
                .attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);
            
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
        
        detailsContainer.innerHTML = `
            <div class="detail-section">
                <h3>Summary</h3>
                <ul class="detail-list">
                    <li><strong>Total Tables:</strong> ${tables.length}</li>
                    <li><strong>Total Columns:</strong> ${columns.length}</li>
                    <li><strong>Total Queries:</strong> ${queries.length}</li>
                    <li><strong>Total Relationships:</strong> ${data.edges ? data.edges.length : 0}</li>
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
            
            ${data.edges && data.edges.length > 0 ? `
            <div class="detail-section">
                <h3>Relationships</h3>
                <ul class="detail-list">
                    ${data.edges.map(e => `
                        <li>
                            <strong>${e.source}</strong> 
                            <em style="color: #666;">${e.type}</em> 
                            <strong>${e.target}</strong>
                        </li>
                    `).join('')}
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