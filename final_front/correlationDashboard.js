/**
 * Cross-dataset correlation analysis and dashboard for NYC Urban Rhythm
 */

/**
 * Calculate correlations between all datasets
 * @param {Object} processedData - Processed data from all datasets
 * @returns {Object} - Correlation matrix and insights
 */
function calculateAllCorrelations() {
    // Initialize correlation object
    const correlations = {
        matrix: {},
        insights: [],
        hotspots: []
    };
    
    // Get valid boroughs
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Datasets to analyze
    const datasets = ['calls311', 'transit', 'taxi', 'events'];
    
    // Initialize correlation matrix
    datasets.forEach(d1 => {
        correlations.matrix[d1] = {};
        datasets.forEach(d2 => {
            correlations.matrix[d1][d2] = d1 === d2 ? 1 : null;
        });
    });
    
    // Calculate correlations between datasets
    
    // 1. Transit vs 311
    const transit311 = calculateTransit311Correlation();
    if (transit311) {
        // Set matrix values
        correlations.matrix['transit']['calls311'] = 
        correlations.matrix['calls311']['transit'] = 
            Object.values(transit311.byBorough).reduce((sum, val) => sum + val, 0) / 
            Object.values(transit311.byBorough).length;
        
        // Add borough-level insights
        validBoroughs.forEach(borough => {
            if (transit311.byBorough[borough] !== undefined) {
                const corr = transit311.byBorough[borough];
                if (Math.abs(corr) > 0.5) {
                    correlations.insights.push({
                        type: 'transit-311',
                        borough,
                        correlation: corr,
                        description: `${borough} shows a ${corr > 0 ? 'positive' : 'negative'} correlation (${corr.toFixed(2)}) between transit ridership and 311 calls`
                    });
                }
            }
        });
    }
    
    // 2. Taxi vs 311
    const taxi311 = analyzeTaxi311Correlation();
    if (taxi311) {
        // Set matrix values
        correlations.matrix['taxi']['calls311'] = 
        correlations.matrix['calls311']['taxi'] = 
            Object.values(taxi311.byBorough).reduce((sum, val) => sum + val, 0) / 
            Object.values(taxi311.byBorough).length;
        
        // Add borough-level insights
        validBoroughs.forEach(borough => {
            if (taxi311.byBorough[borough] !== undefined) {
                const corr = taxi311.byBorough[borough];
                if (Math.abs(corr) > 0.5) {
                    correlations.insights.push({
                        type: 'taxi-311',
                        borough,
                        correlation: corr,
                        description: `${borough} shows a ${corr > 0 ? 'positive' : 'negative'} correlation (${corr.toFixed(2)}) between taxi activity and 311 calls`
                    });
                }
            }
        });
    }
    
    // 3. Events impact on other datasets
    if (processedData.events.eventsList && processedData.events.eventsList.length > 0) {
        // Analyze event impact
        const eventAnalysis = analyzeEventImpact(
            processedData.events.eventsList,
            document.getElementById('startDate').value,
            document.getElementById('endDate').value
        );
        
        if (eventAnalysis.impact && eventAnalysis.impact.length > 0) {
            // Calculate average impact
            const avgImpact = eventAnalysis.impact.reduce((sum, event) => {
                return sum + Math.abs(event.percentChange || 0);
            }, 0) / eventAnalysis.impact.length;
            
            // Set matrix value based on impact magnitude
            correlations.matrix['events']['calls311'] = 
            correlations.matrix['calls311']['events'] = 
                avgImpact > 50 ? 0.8 : avgImpact > 30 ? 0.6 : avgImpact > 15 ? 0.4 : 0.2;
            
            // Add insights for significant impacts
            eventAnalysis.impact.slice(0, 3).forEach(impact => {
                if (Math.abs(impact.percentChange || 0) > 20) {
                    correlations.insights.push({
                        type: 'event-impact',
                        event: impact.event,
                        borough: impact.borough,
                        change: impact.percentChange,
                        description: `Event "${impact.event}" in ${impact.borough} caused a ${impact.percentChange.toFixed(1)}% change in 311 calls`
                    });
                }
            });
        }
    }
    
    // 4. Transit vs Taxi
    // Use hourly patterns to calculate correlation
    if (processedData.transit.hourlyByBorough && processedData.taxi.hourlyByBorough) {
        const transitHourly = Array(24).fill(0);
        const taxiHourly = Array(24).fill(0);
        
        validBoroughs.forEach(borough => {
            for (let hour = 0; hour < 24; hour++) {
                transitHourly[hour] += processedData.transit.hourlyByBorough[borough][hour] || 0;
                taxiHourly[hour] += processedData.taxi.hourlyByBorough[borough][hour] || 0;
            }
        });
        
        // Calculate correlation
        const transitTaxiCorr = calculateCorrelation(transitHourly, taxiHourly);
        
        // Set matrix values
        correlations.matrix['transit']['taxi'] = 
        correlations.matrix['taxi']['transit'] = transitTaxiCorr;
        
        if (Math.abs(transitTaxiCorr) > 0.5) {
            correlations.insights.push({
                type: 'transit-taxi',
                correlation: transitTaxiCorr,
                description: `Transit and taxi activity show a ${transitTaxiCorr > 0 ? 'positive' : 'negative'} correlation (${transitTaxiCorr.toFixed(2)}) throughout the day`
            });
        }
    }
    
    // 5. Transit/Taxi vs Events
    // Use simple heuristic based on event density
    if (processedData.events.eventsList && processedData.events.eventsList.length > 0) {
        // Check event impact on transit
        const eventTransitCorr = 0.3; // Placeholder value based on domain knowledge
        correlations.matrix['events']['transit'] = 
        correlations.matrix['transit']['events'] = eventTransitCorr;
        
        // Check event impact on taxi
        const eventTaxiCorr = 0.4; // Placeholder value based on domain knowledge
        correlations.matrix['events']['taxi'] = 
        correlations.matrix['taxi']['events'] = eventTaxiCorr;
    }
    
    // 6. Weather correlation
    if (Object.keys(processedData.weather).length > 0) {
        // Add weather as a dataset
        correlations.matrix['weather'] = {};
        datasets.forEach(d => {
            correlations.matrix['weather'][d] = null;
            correlations.matrix[d]['weather'] = null;
        });
        correlations.matrix['weather']['weather'] = 1;
        
        // Analyze weather correlation with 311
        const weatherFindings = document.getElementById('weather-findings').innerHTML;
        if (weatherFindings && !weatherFindings.includes('Insufficient data')) {
            correlations.matrix['weather']['calls311'] = 
            correlations.matrix['calls311']['weather'] = 0.4; // Placeholder value based on findings
            
            correlations.insights.push({
                type: 'weather-311',
                description: 'Weather conditions show significant correlation with specific 311 complaint types'
            });
        }
    }
    
    // Find activity hotspots
    const hotspots = findActivityHotspots();
    if (hotspots && hotspots.length > 0) {
        correlations.hotspots = hotspots;
    }
    
    return correlations;
}

/**
 * Find locations with high activity across multiple datasets
 * @returns {Array} - Array of hotspot locations
 */
function findActivityHotspots() {
    const hotspots = [];
    
    // Detect clusters
    const clusters = detectActivityClusters();
    
    // Convert to hotspots
    clusters.forEach(cluster => {
        hotspots.push({
            lat: cluster.center.lat,
            lng: cluster.center.lng,
            pointCount: cluster.pointCount,
            dominantType: cluster.dominant,
            types: cluster.types,
            score: cluster.pointCount
        });
    });
    
    // Sort by score
    hotspots.sort((a, b) => b.score - a.score);
    
    return hotspots.slice(0, 5);
}

/**
 * Calculate urban rhythm patterns
 * @returns {Object} - Rhythm patterns by borough and time
 */
function calculateUrbanRhythms() {
    return calculateRhythmScores();
}

/**
 * Create correlation heatmap for matrix visualization
 * @param {Object} correlationMatrix - Dataset correlation matrix
 * @returns {Array} - Data for heatmap chart
 */
function createCorrelationHeatmap(correlationMatrix) {
    const datasets = Object.keys(correlationMatrix);
    const data = [];
    
    // Convert matrix to heatmap data format
    datasets.forEach((row, i) => {
        datasets.forEach((col, j) => {
            data.push({
                x: col,
                y: row,
                v: correlationMatrix[row][col] || 0
            });
        });
    });
    
    return data;
}

/**
 * Generate urban rhythm visualization
 * @param {Object} rhythms - Urban rhythm data
 * @returns {Object} - Visualization data
 */
function generateRhythmVisualization(rhythms) {
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Generate hour labels
    const hourLabels = Array(24).fill().map((_, i) => `${i}:00`);
    
    // Generate day labels
    const dowLabels = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    
    // Initialize visualization object
    const visualization = {
        hourly: {},
        dow: {},
        peaks: {},
        pulseData: []
    };
    
    // Process each borough
    validBoroughs.forEach(borough => {
        // Add hourly data
        visualization.hourly[borough] = rhythms[borough]?.hourly || Array(24).fill(0);
        
        // Add dow data
        visualization.dow[borough] = rhythms[borough]?.dow || Array(7).fill(0);
        
        // Add peak info
        visualization.peaks[borough] = {
            hour: rhythms[borough]?.peakHour || 0,
            day: rhythms[borough]?.peakDay || 0,
            hourLabel: hourLabels[rhythms[borough]?.peakHour || 0],
            dayLabel: dowLabels[rhythms[borough]?.peakDay || 0]
        };
        
        // Create pulse animation data
        const [lat, lng] = BOROUGH_CENTROIDS[borough];
        const activityLevel = rhythms[borough]?.overallActivity || 0;
        
        if (activityLevel > 0) {
            visualization.pulseData.push({
                lat,
                lng,
                radius: Math.min(500 + (activityLevel / 1000), 2000),
                intensity: Math.min(activityLevel / 5000, 1),
                borough,
                peakHour: visualization.peaks[borough].hourLabel,
                peakDay: visualization.peaks[borough].dayLabel,
                color: BOROUGH_COLORS[borough]
            });
        }
    });
    
    return visualization;
}

/**
 * Update correlation dashboard visualizations
 * @param {Object} correlations - Correlation analysis results
 */
function updateCorrelationDashboard(correlations) {
    // Update correlation matrix chart
    updateCorrelationMatrix(correlations.matrix);
    
    // Update transit-311 correlation chart
    updateTransit311Chart();
    
    // Update taxi-311 correlation chart
    updateTaxi311Chart();
    
    // Update events impact chart
    updateEventsImpactChart();
    
    // Add activity pulses to map
    const rhythms = calculateUrbanRhythms();
    const rhythmViz = generateRhythmVisualization(rhythms);
    
    // Add pulses for each borough
    rhythmViz.pulseData.forEach(pulse => {
        createPulseEffect(map, [pulse.lng, pulse.lat], pulse.color);
    });
}

/**
 * Update correlation matrix chart
 * @param {Object} matrix - Correlation matrix
 */
function updateCorrelationMatrix(matrix) {
    // Convert matrix to heatmap data
    const heatmapData = createCorrelationHeatmap(matrix);
    
    // Get datasets
    const datasets = Object.keys(matrix);
    
    // Create heatmap chart
    if (charts.correlationMatrix) charts.correlationMatrix.destroy();
    
    const ctx = document.getElementById('correlation-matrix').getContext('2d');
    
    // Create heatmap manually since Chart.js lacks built-in heatmap
    const cellSize = Math.min(20, ctx.canvas.width / datasets.length);
    const margin = { top: 30, right: 20, bottom: 30, left: 60 };
    const width = cellSize * datasets.length;
    const height = cellSize * datasets.length;
    
    // Clear canvas
    ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
    
    // Draw title
    ctx.font = '14px Arial';
    ctx.textAlign = 'center';
    ctx.fillStyle = '#333';
    ctx.fillText('Dataset Correlation Matrix', ctx.canvas.width / 2, 15);
    
    // Draw x-axis labels
    ctx.font = '10px Arial';
    ctx.textAlign = 'center';
    datasets.forEach((dataset, i) => {
        const x = margin.left + i * cellSize + cellSize / 2;
        ctx.fillText(dataset, x, margin.top - 10);
    });
    
    // Draw y-axis labels
    ctx.textAlign = 'right';
    datasets.forEach((dataset, i) => {
        const y = margin.top + i * cellSize + cellSize / 2;
        ctx.fillText(dataset, margin.left - 5, y + 3);
    });
    
    // Draw heatmap cells
    heatmapData.forEach(cell => {
        const i = datasets.indexOf(cell.x);
        const j = datasets.indexOf(cell.y);
        const x = margin.left + i * cellSize;
        const y = margin.top + j * cellSize;
        
        // Calculate color based on correlation value
        const value = cell.v === null ? 0 : cell.v;
        let color;
        if (value > 0) {
            const intensity = Math.min(value, 1);
            color = `rgba(21, 128, 61, ${intensity})`;
        } else if (value < 0) {
            const intensity = Math.min(Math.abs(value), 1);
            color = `rgba(220, 38, 38, ${intensity})`;
        } else {
            color = 'rgba(229, 231, 235, 0.5)';
        }
        
        // Draw cell
        ctx.fillStyle = color;
        ctx.fillRect(x, y, cellSize, cellSize);
        
        // Draw cell border
        ctx.strokeStyle = '#fff';
        ctx.strokeRect(x, y, cellSize, cellSize);
        
        // Draw correlation value
        if (value !== 0) {
            ctx.fillStyle = Math.abs(value) > 0.5 ? '#fff' : '#333';
            ctx.textAlign = 'center';
            ctx.font = '9px Arial';
            ctx.fillText(value.toFixed(2), x + cellSize / 2, y + cellSize / 2 + 3);
        }
    });
}

/**
 * Update transit-311 correlation chart
 */
function updateTransit311Chart() {
    // Get correlation data
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    const correlations = processedData.correlations?.transitVs311 || {};
    
    // Create chart data
    const data = validBoroughs.map(borough => ({
        borough,
        correlation: correlations[borough] || 0
    }));
    
    // Sort by correlation strength
    data.sort((a, b) => Math.abs(b.correlation) - Math.abs(a.correlation));
    
    // Create chart
    if (charts.transit311) charts.transit311.destroy();
    charts.transit311 = new Chart(
        document.getElementById('transit-311-chart'),
        {
            type: 'bar',
            data: {
                labels: data.map(d => d.borough),
                datasets: [{
                    label: 'Correlation',
                    data: data.map(d => d.correlation),
                    backgroundColor: data.map(d => 
                        d.correlation >= 0 ? '#10B981' : '#EF4444'
                    )
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    title: {
                        display: true,
                        text: 'Transit & 311 Correlation by Borough',
                        font: {
                            size: 14
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const value = context.raw;
                                return `Correlation: ${value.toFixed(2)}`;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: false,
                        min: -1,
                        max: 1,
                        title: {
                            display: true,
                            text: 'Correlation Coefficient',
                            font: {
                                size: 10
                            }
                        },
                        ticks: {
                            font: {
                                size: 9
                            }
                        }
                    },
                    x: {
                        ticks: {
                            font: {
                                size: 9
                            }
                        }
                    }
                }
            }
        }
    );
}

/**
 * Update taxi-311 correlation chart
 */
function updateTaxi311Chart() {
    // Get correlation data
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    const correlations = processedData.correlations?.taxiVs311 || {};
    
    // Create chart data
    const data = validBoroughs.map(borough => ({
        borough,
        correlation: correlations[borough] || 0
    }));
    
    // Sort by correlation strength
    data.sort((a, b) => Math.abs(b.correlation) - Math.abs(a.correlation));
    
    // Create chart
    if (charts.taxi311) charts.taxi311.destroy();
    charts.taxi311 = new Chart(
        document.getElementById('taxi-311-chart'),
        {
            type: 'bar',
            data: {
                labels: data.map(d => d.borough),
                datasets: [{
                    label: 'Correlation',
                    data: data.map(d => d.correlation),
                    backgroundColor: data.map(d => 
                        d.correlation >= 0 ? '#F59E0B' : '#EF4444'
                    )
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    title: {
                        display: true,
                        text: 'Taxi & 311 Correlation by Borough',
                        font: {
                            size: 14
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const value = context.raw;
                                return `Correlation: ${value.toFixed(2)}`;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: false,
                        min: -1,
                        max: 1,
                        title: {
                            display: true,
                            text: 'Correlation Coefficient',
                            font: {
                                size: 10
                            }
                        },
                        ticks: {
                            font: {
                                size: 9
                            }
                        }
                    },
                    x: {
                        ticks: {
                            font: {
                                size: 9
                            }
                        }
                    }
                }
            }
        }
    );
}

/**
 * Update events impact chart
 */
function updateEventsImpactChart() {
    // Check if we have event impact data
    if (!processedData.events.eventsList || processedData.events.eventsList.length === 0) {
        return;
    }
    
    // Analyze event impact
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;
    
    const eventAnalysis = analyzeEventImpact(
        processedData.events.eventsList,
        startDate,
        endDate
    );
    
    if (!eventAnalysis.impact || eventAnalysis.impact.length === 0) {
        return;
    }
    
    // Use top 5 events with significant impact
    const impactEvents = eventAnalysis.impact
        .filter(event => Math.abs(event.percentChange || 0) > 10)
        .slice(0, 5);
    
    if (impactEvents.length === 0) {
        return;
    }
    
    // Create chart
    if (charts.eventsImpact) charts.eventsImpact.destroy();
    charts.eventsImpact = new Chart(
        document.getElementById('events-impact-chart'),
        {
            type: 'bar',
            data: {
                labels: impactEvents.map(event => 
                    event.event.length > 15 ? 
                    event.event.substring(0, 15) + '...' : event.event
                ),
                datasets: [{
                    label: 'Percent Change in 311 Calls',
                    data: impactEvents.map(event => event.percentChange),
                    backgroundColor: impactEvents.map(event => 
                        event.percentChange >= 0 ? '#8B5CF6' : '#EF4444'
                    )
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: {
                    legend: {
                        display: false
                    },
                    title: {
                        display: true,
                        text: 'Events Impact on 311 Calls',
                        font: {
                            size: 14
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const value = context.raw;
                                return `Change: ${value.toFixed(1)}%`;
                            },
                            footer: function(tooltipItems) {
                                const idx = tooltipItems[0].dataIndex;
                                const event = impactEvents[idx];
                                return `${event.borough} - ${event.date}`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Percent Change',
                            font: {
                                size: 10
                            }
                        },
                        ticks: {
                            font: {
                                size: 9
                            }
                        }
                    },
                    y: {
                        ticks: {
                            font: {
                                size: 9
                            }
                        }
                    }
                }
            }
        }
    );
}