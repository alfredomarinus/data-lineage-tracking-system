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
            this.currentData = data;
            
            this.visualizeData(data);
            this.updateDetails(data);
            this.updateJSON(data);
            
            // Switch to visualization tab
            document.querySelector('.tab-btn[data-tab="visualization"]').click();
            
        } catch (error) {
            this.showError(error.message);
        } finally {
            this.showLoading(false);
        }
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
        
        // Create SVG
        const svg = d3.select('#graph-container')
            .append('svg')
            .attr('id', 'graph-svg')
            .attr('width', '100%')
            .attr('height', '100%');
        
        const width = container.clientWidth;
        const height = Math.max(500, container.clientHeight);
        
        svg.attr('viewBox', `0 0 ${width} ${height}`);
        
        // Define arrow markers
        svg.append('defs').append('marker')
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
        
        // Create force simulation
        this.simulation = d3.forceSimulation(data.nodes)
            .force('link', d3.forceLink(data.edges).id(d => d.id).distance(100))
            .force('charge', d3.forceManyBody().strength(-300))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(30));
        
        // Create links
        const link = svg.append('g')
            .selectAll('line')
            .data(data.edges)
            .join('line')
            .attr('class', d => `link ${d.type}`)
            .attr('stroke-width', 2);
        
        // Create nodes
        const node = svg.append('g')
            .selectAll('g')
            .data(data.nodes)
            .join('g')
            .attr('class', d => `node ${d.type}`)
            .call(this.drag(this.simulation));
        
        node.append('circle')
            .attr('r', d => d.type === 'query' ? 15 : 12)
            .attr('fill', d => this.getNodeColor(d.type));
        
        node.append('text')
            .attr('dy', 25)
            .attr('text-anchor', 'middle')
            .text(d => this.truncateText(d.name, 15))
            .style('font-size', '11px')
            .style('fill', '#333');
        
        // Add tooltips
        node.append('title')
            .text(d => `${d.type.toUpperCase()}: ${d.name}${d.schema ? ` (${d.schema})` : ''}`);
        
        // Update positions on simulation tick
        this.simulation.on('tick', () => {
            link
                .attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);
            
            node.attr('transform', d => `translate(${d.x},${d.y})`);
        });
        
        // Add legend
        this.addLegend(container);
    }
    
    addLegend(container) {
        const legend = d3.select(container)
            .append('div')
            .attr('class', 'legend');
        
        const legendData = [
            { type: 'table', color: '#4CAF50', label: 'Table' },
            { type: 'column', color: '#2196F3', label: 'Column' },
            { type: 'query', color: '#FF9800', label: 'Query' }
        ];
        
        const legendItems = legend.selectAll('.legend-item')
            .data(legendData)
            .join('div')
            .attr('class', 'legend-item');
        
        legendItems.append('div')
            .attr('class', 'legend-color')
            .style('background-color', d => d.color);
        
        legendItems.append('span')
            .text(d => d.label);
    }
    
    getNodeColor(type) {
        const colors = {
            'table': '#4CAF50',
            'column': '#2196F3',
            'query': '#FF9800'
        };
        return colors[type] || '#666';
    }
    
    truncateText(text, maxLength) {
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
                    <li><strong>Total Relationships:</strong> ${data.edges.length}</li>
                </ul>
            </div>
            
            <div class="detail-section">
                <h3>Tables</h3>
                <div>
                    ${tables.map(t => `
                        <span class="table-info">
                            ${t.schema ? `${t.schema}.` : ''}${t.name}
                        </span>
                    `).join('')}
                </div>
            </div>
            
            <div class="detail-section">
                <h3>Columns</h3>
                <div>
                    ${columns.map(c => `
                        <span class="column-info">
                            ${c.schema ? `${c.schema}.` : ''}${c.name}
                        </span>
                    `).join('')}
                </div>
            </div>
            
            <div class="detail-section">
                <h3>Relationships</h3>
                <ul class="detail-list">
                    ${data.edges.map(e => `
                        <li>
                            <strong>${e.source}</strong> 
                            <em>${e.type}</em> 
                            <strong>${e.target}</strong>
                        </li>
                    `).join('')}
                </ul>
            </div>
        `;
    }
    
    updateJSON(data) {
        const jsonOutput = document.getElementById('json-output');
        jsonOutput.textContent = JSON.stringify(data, null, 2);
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
        
        document.getElementById('sql-input').value = exampleSQL;
    }
    
    clearAll() {
        document.getElementById('sql-input').value = '';
        document.getElementById('graph-container').innerHTML = `
            <div id="graph-placeholder">
                <p>Enter a SQL query above and click "Parse & Visualize" to see the data lineage graph.</p>
            </div>
        `;
        document.getElementById('details-content').innerHTML = '<p>Query details will appear here after parsing.</p>';
        document.getElementById('json-output').textContent = 'JSON output will appear here after parsing.';
        this.currentData = null;
    }
    
    showLoading(show) {
        const loading = document.getElementById('loading');
        loading.classList.toggle('hidden', !show);
    }
    
    showError(message) {
        const errorDiv = document.getElementById('error-message');
        errorDiv.textContent = message;
        errorDiv.classList.remove('hidden');
        
        setTimeout(() => {
            errorDiv.classList.add('hidden');
        }, 5000);
    }
}

// Initialize the visualizer when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new SQLLineageVisualizer();
});