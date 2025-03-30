/**
 * Chart creation and updates for NYC Urban Rhythm
 */

// Chart instances
let charts = {
    trends: null,
    dow: null,
    hour: null,
    proportion: null,
    categories: null,
    transitBorough: null,
    transitTime: null,
    taxiTime: null,
    taxiDistance: null,
    taxiLocations: null,
    eventsBorough: null,
    eventsType: null,
    transit311: null,
    eventsImpact: null,
    taxi311: null,
    correlationMatrix: null,
    temp: null,
    precip: null
};

/**
 * Initialize all charts
 */
function initializeCharts() {
    // Set Chart.js defaults
    Chart.defaults.font.family = "'Inter', sans-serif";
    Chart.defaults.font.size = 12;
    Chart.defaults.color = '#4B5563';
    Chart.defaults.plugins.tooltip.backgroundColor = 'rgba(255, 255, 255, 0.9)';
    Chart.defaults.plugins.tooltip.titleColor = '#111827';
    Chart.defaults.plugins.tooltip.bodyColor = '#4B5563';
    Chart.defaults.plugins.tooltip.borderColor = '#E5E7EB';
    Chart.defaults.plugins.tooltip.borderWidth = 1;
    Chart.defaults.plugins.tooltip.padding = 10;
    Chart.defaults.plugins.tooltip.displayColors = true;
}

/**
 * Update all charts based on processed data
 */
function updateAllCharts() {
    updateTrendCharts();
    updateCategoryCharts();
    updateTransitCharts();
    updateTaxiCharts();
    updateEventCharts();
    updateCorrelationCharts();
    updateWeatherCharts();
}

/**
 * Update trend analysis charts
 */
function updateTrendCharts() {
    // Check if we have daily data
    if (!processedData.calls311.dailyData) return;
    
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    const days = Object.keys(processedData.calls311.dailyData).sort();
    
    // If too many days, sample to avoid overcrowding
    let displayDays = days;
    let displayData = processedData.calls311.dailyData;
    
    if (days.length > 14) {
        // Sample at most 14 days for clarity
        const sampleInterval = Math.max(1, Math.floor(days.length / 14));
        displayDays = days.filter((_, i) => i % sampleInterval === 0);
        
        // Filter data for sampled days
        displayData = {};
        displayDays.forEach(day => {
            displayData[day] = processedData.calls311.dailyData[day];
        });
    }
    
    // Daily trend chart
    const trendDatasets = validBoroughs.map(borough => ({
        label: borough,
        data: displayDays.map(day => displayData[day].byBorough[borough] || 0),
        borderColor: BOROUGH_COLORS[borough],
        backgroundColor: BOROUGH_COLORS[borough] + '40',
        tension: 0.2
    }));
    
    if (charts.trends) charts.trends.destroy();
    charts.trends = new Chart(
        document.getElementById('trends-chart'),
        {
            type: 'line',
            data: {
                labels: displayDays,
                datasets: trendDatasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false,
                },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            boxWidth: 12,
                            font: {
                                size: 10
                            }
                        }
                    },
                    title: {
                        display: true,
                        text: '311 Calls Over Time',
                        font: {
                            size: 14
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Number of Calls',
                            font: {
                                size: 10
                            }
                        }
                    },
                    x: {
                        title: {
                            display: true,
                            text: 'Date',
                            font: {
                                size: 10
                            }
                        },
                        ticks: {
                            maxRotation: 45,
                            minRotation: 45,
                            font: {
                                size: 9
                            }
                        }
                    }
                }
            }
        }
    );
    
    // Day of week chart
    const dowLabels = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    const dowDatasets = [
        {
            label: '311 Calls',
            data: Array(7).fill(0),
            backgroundColor: DATASET_COLORS.calls311
        }
    ];
    
    // Add transit data if available
    if (processedData.transit.dowByBorough) {
        dowDatasets.push({
            label: 'Transit Entries',
            data: Array(7).fill(0),
            backgroundColor: DATASET_COLORS.transit
        });
    }
    
    // Add taxi data if available
    if (processedData.taxi.dowByBorough) {
        dowDatasets.push({
            label: 'Taxi Pickups',
            data: Array(7).fill(0),
            backgroundColor: DATASET_COLORS.taxi
        });
    }
    
    // Aggregate calls by day of week
    validBoroughs.forEach(borough => {
        if (processedData.calls311.dowByBorough) {
            processedData.calls311.dowByBorough[borough].forEach((count, dow) => {
                dowDatasets[0].data[dow] += count;
            });
        }
        
        // Add transit data if available
        if (processedData.transit.dowByBorough && dowDatasets.length > 1) {
            processedData.transit.dowByBorough[borough].forEach((count, dow) => {
                dowDatasets[1].data[dow] += count * 0.1; // Scale down transit data
            });
        }
        
        // Add taxi data if available
        if (processedData.taxi.dowByBorough && dowDatasets.length > 2) {
            processedData.taxi.dowByBorough[borough].forEach((count, dow) => {
                dowDatasets[2].data[dow] += count;
            });
        }
    });
    
    if (charts.dow) charts.dow.destroy();
    charts.dow = new Chart(
        document.getElementById('dow-chart'),
        {
            type: 'bar',
            data: {
                labels: dowLabels,
                datasets: dowDatasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            boxWidth: 12,
                            font: {
                                size: 10
                            }
                        }
                    },
                    title: {
                        display: true,
                        text: 'Activity by Day of Week',
                        font: {
                            size: 14
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Activity Count',
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
    
    // Hour of day chart
    const hourLabels = Array(24).fill().map((_, i) => i);
    const hourDatasets = validBoroughs.map(borough => ({
        label: borough,
        data: processedData.calls311.hourlyByBorough[borough],
        borderColor: BOROUGH_COLORS[borough],
        backgroundColor: BOROUGH_COLORS[borough] + '40',
        pointRadius: 1,
    }));
    
    if (charts.hour) charts.hour.destroy();
    charts.hour = new Chart(
        document.getElementById('hour-chart'),
        {
            type: 'line',
            data: {
                labels: hourLabels,
                datasets: hourDatasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            boxWidth: 12,
                            font: {
                                size: 10
                            }
                        }
                    },
                    title: {
                        display: true,
                        text: '311 Calls by Hour of Day',
                        font: {
                            size: 14
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Number of Calls',
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
                        title: {
                            display: true,
                            text: 'Hour of Day',
                            font: {
                                size: 10
                            }
                        },
                        ticks: {
                            stepSize: 2,
                            font: {
                                size: 9
                            },
                            callback: function(value) {
                                return `${value}:00`;
                            }
                        }
                    }
                }
            }
        }
    );
    
    // Borough proportion over time
    const proportionDatasets = validBoroughs.map(borough => ({
        label: borough,
        data: displayDays.map(day => {
            const total = displayData[day].total;
            return total > 0 ? (displayData[day].byBorough[borough] / total) * 100 : 0;
        }),
        borderColor: BOROUGH_COLORS[borough],
        backgroundColor: BOROUGH_COLORS[borough] + '40',
        pointRadius: 1,
    }));
    
    if (charts.proportion) charts.proportion.destroy();
    charts.proportion = new Chart(
        document.getElementById('proportion-chart'),
        {
            type: 'line',
            data: {
                labels: displayDays,
                datasets: proportionDatasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false,
                },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            boxWidth: 12,
                            font: {
                                size: 10
                            }
                        }
                    },
                    title: {
                        display: true,
                        text: 'Borough Proportion Over Time',
                        font: {
                            size: 14
                        }
                    }
                },
                scales: {
                    y: {
                        stacked: false,
                        title: {
                            display: true,
                            text: 'Percentage of Daily Calls',
                            font: {
                                size: 10
                            }
                        },
                        min: 0,
                        max: 100,
                        ticks: {
                            font: {
                                size: 9
                            }
                        }
                    },
                    x: {
                        title: {
                            display: true,
                            text: 'Date',
                            font: {
                                size: 10
                            }
                        },
                        ticks: {
                            maxRotation: 45,
                            minRotation: 45,
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
 * Update category charts
 */
function updateCategoryCharts() {
    // Check if we have complaint types data
    if (!processedData.calls311.complaintTypes) return;
    
    // Populate categories list
    const categoriesList = document.getElementById('categories-list');
    categoriesList.innerHTML = '';
    
    // Get sorted complaints
    const sortedComplaints = Object.entries(processedData.calls311.complaintTypes)
        .sort((a, b) => b[1].total - a[1].total);
    
    // Create list items for top 20 complaints
    sortedComplaints.slice(0, 20).forEach(([complaint, data]) => {
        const div = document.createElement('div');
        div.className = 'category-item p-2 border-b flex justify-between items-center';
        
        div.innerHTML = `
            <span class="complaint-name">${complaint}</span>
            <span class="text-gray-700 font-medium">${formatNumber(data.total)}</span>
        `;
        
        div.addEventListener('click', () => {
            // Update selected state
            document.querySelectorAll('.category-item').forEach(el => 
                el.classList.remove('selected')
            );
            div.classList.add('selected');
            
            // Update category chart
            updateCategoryChart(complaint, data);
        });
        
        categoriesList.appendChild(div);
    });
    
    // Select first category by default
    if (sortedComplaints.length > 0) {
        const [firstComplaint, firstData] = sortedComplaints[0];
        document.querySelector('.category-item')?.classList.add('selected');
        updateCategoryChart(firstComplaint, firstData);
    }
}

/**
 * Update category breakdown chart
 * @param {string} complaint - Complaint type
 * @param {Object} data - Complaint data
 */
function updateCategoryChart(complaint, data) {
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    if (charts.categories) charts.categories.destroy();
    charts.categories = new Chart(
        document.getElementById('categories-chart'),
        {
            type: 'bar',
            data: {
                labels: validBoroughs,
                datasets: [{
                    label: complaint,
                    data: validBoroughs.map(borough => data.byBorough[borough] || 0),
                    backgroundColor: validBoroughs.map(borough => BOROUGH_COLORS[borough]),
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
                        text: complaint.length > 40 ? complaint.substring(0, 40) + '...' : complaint,
                        font: {
                            size: 14
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Number of Complaints',
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
 * Update transit charts
 */
function updateTransitCharts() {
    // Check if we have transit data
    if (!processedData.transit.ridershipByBorough) return;
    
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Transit by borough chart
    if (charts.transitBorough) charts.transitBorough.destroy();
    charts.transitBorough = new Chart(
        document.getElementById('transit-borough-chart'),
        {
            type: 'pie',
            data: {
                labels: validBoroughs,
                datasets: [{
                    data: validBoroughs.map(borough => 
                        processedData.transit.ridershipByBorough[borough] || 0
                    ),
                    backgroundColor: validBoroughs.map(borough => BOROUGH_COLORS[borough]),
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'right',
                        labels: {
                            boxWidth: 12,
                            font: {
                                size: 10
                            }
                        }
                    },
                    title: {
                        display: true,
                        text: 'Ridership by Borough',
                        font: {
                            size: 14
                        }
                    }
                }
            }
        }
    );
    
    // Transit by time of day chart
    const hourLabels = Array(24).fill().map((_, i) => i);
    const timeDatasets = validBoroughs.map(borough => ({
        label: borough,
        data: processedData.transit.hourlyByBorough?.[borough] || Array(24).fill(0),
        borderColor: BOROUGH_COLORS[borough],
        backgroundColor: BOROUGH_COLORS[borough] + '40',
        pointRadius: 1,
    }));
    
    if (charts.transitTime) charts.transitTime.destroy();
    charts.transitTime = new Chart(
        document.getElementById('transit-time-chart'),
        {
            type: 'line',
            data: {
                labels: hourLabels,
                datasets: timeDatasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            boxWidth: 12,
                            font: {
                                size: 10
                            }
                        }
                    },
                    title: {
                        display: true,
                        text: 'Ridership by Hour of Day',
                        font: {
                            size: 14
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Number of Entries',
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
                        title: {
                            display: true,
                            text: 'Hour of Day',
                            font: {
                                size: 10
                            }
                        },
                        ticks: {
                            stepSize: 2,
                            font: {
                                size: 9
                            },
                            callback: function(value) {
                                return `${value}:00`;
                            }
                        }
                    }
                }
            }
        }
    );
    
    // Top stations list
    const topStations = document.getElementById('top-stations');
    topStations.innerHTML = '';
    
    // Get sorted stations
    const sortedStations = Object.entries(processedData.transit.stationRidership || {})
        .sort((a, b) => b[1].total - a[1].total);
    
    // Create list items for top stations
    sortedStations.slice(0, 9).forEach(([station, data]) => {
        const div = document.createElement('div');
        div.className = 'p-2 bg-gray-50 rounded-lg text-xs';
        
        div.innerHTML = `
            <div class="font-medium">${station}</div>
            <div class="text-gray-600">${data.borough}</div>
            <div class="font-medium text-green-800 mt-1">${formatNumber(Math.round(data.total))} entries</div>
        `;
        
        topStations.appendChild(div);
    });
}

/**
 * Update taxi charts
 */
function updateTaxiCharts() {
    // Check if we have taxi data
    if (!processedData.taxi.pickupsByBorough) return;
    
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Trip volume by time chart
    const hourLabels = Array(24).fill().map((_, i) => i);
    const timeDatasets = validBoroughs.map(borough => ({
        label: borough,
        data: processedData.taxi.hourlyByBorough?.[borough] || Array(24).fill(0),
        borderColor: BOROUGH_COLORS[borough],
        backgroundColor: BOROUGH_COLORS[borough] + '40',
        pointRadius: 1,
    }));
    
    if (charts.taxiTime) charts.taxiTime.destroy();
    charts.taxiTime = new Chart(
        document.getElementById('taxi-time-chart'),
        {
            type: 'line',
            data: {
                labels: hourLabels,
                datasets: timeDatasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            boxWidth: 12,
                            font: {
                                size: 10
                            }
                        }
                    },
                    title: {
                        display: true,
                        text: 'Trip Volume by Hour of Day',
                        font: {
                            size: 14
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Number of Pickups',
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
                        title: {
                            display: true,
                            text: 'Hour of Day',
                            font: {
                                size: 10
                            }
                        },
                        ticks: {
                            stepSize: 2,
                            font: {
                                size: 9
                            },
                            callback: function(value) {
                                return `${value}:00`;
                            }
                        }
                    }
                }
            }
        }
    );
    
    // Average trip distance chart
    if (charts.taxiDistance) charts.taxiDistance.destroy();
    charts.taxiDistance = new Chart(
        document.getElementById('taxi-distance-chart'),
        {
            type: 'bar',
            data: {
                labels: validBoroughs,
                datasets: [{
                    label: 'Average Trip Distance (miles)',
                    data: validBoroughs.map(borough => 
                        processedData.taxi.metrics?.distanceByBorough?.[borough] || 0
                    ),
                    backgroundColor: validBoroughs.map(borough => BOROUGH_COLORS[borough]),
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
                        text: 'Average Trip Distance by Borough',
                        font: {
                            size: 14
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Miles',
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
    
    // Popular pickup/dropoff zones chart
    const pickupData = validBoroughs.map(borough => 
        processedData.taxi.pickupsByBorough[borough] || 0
    );
    
    const dropoffData = validBoroughs.map(borough => 
        processedData.taxi.dropoffsByBorough[borough] || 0
    );
    
    if (charts.taxiLocations) charts.taxiLocations.destroy();
    charts.taxiLocations = new Chart(
        document.getElementById('taxi-locations-chart'),
        {
            type: 'bar',
            data: {
                labels: validBoroughs,
                datasets: [
                    {
                        label: 'Pickups',
                        data: pickupData,
                        backgroundColor: '#F59E0B',
                    },
                    {
                        label: 'Dropoffs',
                        data: dropoffData,
                        backgroundColor: '#60A5FA',
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            boxWidth: 12,
                            font: {
                                size: 10
                            }
                        }
                    },
                    title: {
                        display: true,
                        text: 'Pickup vs Dropoff Locations',
                        font: {
                            size: 14
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Number of Trips',
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
 * Update event charts
 */
function updateEventCharts() {
    // Check if we have events data
    if (!processedData.events.eventsByBorough) return;
    
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Events by borough chart
    if (charts.eventsBorough) charts.eventsBorough.destroy();
    charts.eventsBorough = new Chart(
        document.getElementById('events-borough-chart'),
        {
            type: 'pie',
            data: {
                labels: validBoroughs,
                datasets: [{
                    data: validBoroughs.map(borough => 
                        processedData.events.eventsByBorough[borough] || 0
                    ),
                    backgroundColor: validBoroughs.map(borough => BOROUGH_COLORS[borough]),
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'right',
                        labels: {
                            boxWidth: 12,
                            font: {
                                size: 10
                            }
                        }
                    },
                    title: {
                        display: true,
                        text: 'Events by Borough',
                        font: {
                            size: 14
                        }
                    }
                }
            }
        }
    );
    
    // Events by type chart
    if (!processedData.events.eventTypes) return;
    
    // Get top event types
    const sortedTypes = Object.entries(processedData.events.eventTypes)
        .sort((a, b) => b[1].total - a[1].total)
        .slice(0, 5);
    
    if (charts.eventsType) charts.eventsType.destroy();
    charts.eventsType = new Chart(
        document.getElementById('events-type-chart'),
        {
            type: 'bar',
            data: {
                labels: sortedTypes.map(([type]) => type),
                datasets: [{
                    label: 'Number of Events',
                    data: sortedTypes.map(([_, data]) => data.total),
                    backgroundColor: sortedTypes.map((_, i) => 
                        `hsl(${270 + (i * 20)}, 70%, 60%)`
                    ),
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
                        text: 'Top Event Types',
                        font: {
                            size: 14
                        }
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Number of Events',
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
                            },
                            callback: function(value) {
                                const label = this.getLabelForValue(value);
                                return label.length > 15 ? label.substring(0, 15) + '...' : label;
                            }
                        }
                    }
                }
            }
        }
    );
    
    // Upcoming/recent events list
    const eventsList = document.getElementById('events-list');
    eventsList.innerHTML = '';
    
    // Get sorted events by date
    const sortedEvents = [...(processedData.events.eventsList || [])]
        .sort((a, b) => new Date(a.start) - new Date(b.start));
    
    // Create list items for events
    sortedEvents.slice(0, 10).forEach(event => {
        const div = document.createElement('div');
        div.className = 'p-2 border-b';
        
        const startDate = new Date(event.start);
        const formattedDate = startDate.toLocaleDateString();
        
        div.innerHTML = `
            <div class="font-medium text-sm">${event.name}</div>
            <div class="text-xs flex justify-between mt-1">
                <span>${formattedDate}</span>
                <span class="font-medium">${event.borough}</span>
            </div>
        `;
        
        eventsList.appendChild(div);
    });
}

/**
 * Update correlation charts
 */
function updateCorrelationCharts() {
    // Calculate correlations
    const correlations = calculateDatasetCorrelations();
    
    // Transit vs 311 correlation chart
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    if (charts.transit311) charts.transit311.destroy();
    charts.transit311 = new Chart(
        document.getElementById('transit-311-chart'),
        {
            type: 'bar',
            data: {
                labels: validBoroughs,
                datasets: [{
                    label: 'Correlation Coefficient',
                    data: validBoroughs.map(borough => 
                        correlations.transitVs311[borough] || 0
                    ),
                    backgroundColor: validBoroughs.map(borough => {
                        const corr = correlations.transitVs311[borough] || 0;
                        return corr >= 0 ? '#10B981' : '#EF4444';
                    }),
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
                        text: 'Transit & 311 Call Correlation by Borough',
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
    
    // Taxi vs 311 correlation chart
    if (charts.taxi311) charts.taxi311.destroy();
    charts.taxi311 = new Chart(
        document.getElementById('taxi-311-chart'),
        {
            type: 'bar',
            data: {
                labels: validBoroughs,
                datasets: [{
                    label: 'Correlation Coefficient',
                    data: validBoroughs.map(borough => 
                        correlations.taxiVs311[borough] || 0
                    ),
                    backgroundColor: validBoroughs.map(borough => {
                        const corr = correlations.taxiVs311[borough] || 0;
                        return corr >= 0 ? '#F59E0B' : '#EF4444';
                    }),
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
                        text: 'Taxi & 311 Call Correlation by Borough',
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
    
    // Events impact chart
    if (correlations.eventsImpact && correlations.eventsImpact.length > 0) {
        const impactEvents = correlations.eventsImpact.slice(0, 5);
        
        if (charts.eventsImpact) charts.eventsImpact.destroy();
        charts.eventsImpact = new Chart(
            document.getElementById('events-impact-chart'),
            {
                type: 'bar',
                data: {
                    labels: impactEvents.map(event => event.event.length > 15 ? 
                        event.event.substring(0, 15) + '...' : event.event
                    ),
                    datasets: [{
                        label: 'Percent Change in 311 Calls',
                        data: impactEvents.map(event => event.percentChange),
                        backgroundColor: impactEvents.map(event => 
                            event.percentChange >= 0 ? '#8B5CF6' : '#EF4444'
                        ),
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
    
    // Correlation matrix
    const datasets = ['calls311', 'transit', 'taxi', 'events'];
    const matrix = [];
    
    // Calculate all pair-wise correlations
    for (let i = 0; i < datasets.length; i++) {
        matrix.push([]);
        
        for (let j = 0; j < datasets.length; j++) {
            if (i === j) {
                // Perfect correlation with self
                matrix[i].push(1);
            } else {
                // Calculate average borough correlation
                let sum = 0, count = 0;
                
                validBoroughs.forEach(borough => {
                    if (datasets[i] === 'calls311' && datasets[j] === 'transit') {
                        if (correlations.transitVs311 && correlations.transitVs311[borough] !== undefined) {
                            sum += correlations.transitVs311[borough];
                            count++;
                        }
                    } else if (datasets[i] === 'transit' && datasets[j] === 'calls311') {
                        if (correlations.transitVs311 && correlations.transitVs311[borough] !== undefined) {
                            sum += correlations.transitVs311[borough];
                            count++;
                        }
                    } else if (datasets[i] === 'calls311' && datasets[j] === 'taxi') {
                        if (correlations.taxiVs311 && correlations.taxiVs311[borough] !== undefined) {
                            sum += correlations.taxiVs311[borough];
                            count++;
                        }
                    } else if (datasets[i] === 'taxi' && datasets[j] === 'calls311') {
                        if (correlations.taxiVs311 && correlations.taxiVs311[borough] !== undefined) {
                            sum += correlations.taxiVs311[borough];
                            count++;
                        }
                    } else {
                        // For other combinations, use a default low correlation
                        sum += 0.1;
                        count++;
                    }
                });
                
                matrix[i].push(count > 0 ? sum / count : 0);
            }
        }
    }
    
    if (charts.correlationMatrix) charts.correlationMatrix.destroy();   
    charts.correlationMatrix = new Chart(
        document.getElementById('correlation-matrix'),
        {
            type: 'heatmap',
            data: {
                labels: ['311 Calls', 'Transit', 'Taxi', 'Events'],
                datasets: [{
                    label: 'Correlation',
                    data: matrix.flat(),
                    width: 4,
                    height: 4
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
                        text: 'Dataset Correlation Matrix',
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
                    x: {
                        type: 'category',
                        labels: ['311 Calls', 'Transit', 'Taxi', 'Events'],
                        ticks: {
                            font: {
                                size: 9
                            }
                        }
                    },
                    y: {
                        type: 'category',
                        labels: ['311 Calls', 'Transit', 'Taxi', 'Events'],
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
 * Update weather correlation charts
 */
function updateWeatherCharts() {
    // Check if we have weather data
    if (Object.keys(processedData.weather).length === 0) {
        document.getElementById('weather-findings').innerHTML = 
            '<p class="text-center p-4">Weather data not available. Please update dashboard first.</p>';
        
        // Clear charts if they exist
        if (charts.temp) {
            charts.temp.destroy();
            charts.temp = null;
        }
        if (charts.precip) {
            charts.precip.destroy();
            charts.precip = null;
        }
        return;
    }
    
    // Prepare temp vs call data
    const tempData = [];
    const precipData = [];
    
    Object.entries(processedData.weather).forEach(([date, data]) => {
        if (data.tmax !== null && processedData.calls311.dailyData && processedData.calls311.dailyData[date]) {
            tempData.push({
                x: data.tmax,
                y: processedData.calls311.dailyData[date].total,
                date: date
            });
        }
        
        if (data.prcp !== null && processedData.calls311.dailyData && processedData.calls311.dailyData[date]) {
            precipData.push({
                x: data.prcp,
                y: processedData.calls311.dailyData[date].total,
                date: date
            });
        }
    });
    
    // Temperature correlation chart
    if (charts.temp) charts.temp.destroy();
    
    if (tempData.length === 0) {
        // Handle empty temperature data
        document.getElementById('temp-correlation').getContext('2d').clearRect(
            0, 0, 
            document.getElementById('temp-correlation').width, 
            document.getElementById('temp-correlation').height
        );
    } else {
        charts.temp = new Chart(
            document.getElementById('temp-correlation'),
            {
                type: 'scatter',
                data: {
                    datasets: [{
                        label: 'Daily Calls vs Max Temp',
                        data: tempData,
                        backgroundColor: '#3B82F6',
                        pointRadius: 4,
                        pointHoverRadius: 6
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
                            text: 'Temperature vs. 311 Call Volume',
                            font: {
                                size: 14
                            }
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    const point = context.raw;
                                    return `Date: ${point.date}, Temp: ${point.x}°F, Calls: ${point.y}`;
                                }
                            }
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            title: {
                                display: true,
                                text: 'Number of 311 Calls',
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
                            title: {
                                display: true,
                                text: 'Maximum Temperature (°F)',
                                font: {
                                    size: 10
                                }
                            },
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
    
    // Precipitation correlation chart
    if (charts.precip) charts.precip.destroy();
    
    if (precipData.length === 0) {
        // Handle empty precipitation data
        document.getElementById('precip-correlation').getContext('2d').clearRect(
            0, 0, 
            document.getElementById('precip-correlation').width, 
            document.getElementById('precip-correlation').height
        );
    } else {
        charts.precip = new Chart(
            document.getElementById('precip-correlation'),
            {
                type: 'scatter',
                data: {
                    datasets: [{
                        label: 'Daily Calls vs Precipitation',
                        data: precipData,
                        backgroundColor: '#10B981',
                        pointRadius: 4,
                        pointHoverRadius: 6
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
                            text: 'Precipitation vs. 311 Call Volume',
                            font: {
                                size: 14
                            }
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    const point = context.raw;
                                    return `Date: ${point.date}, Precip: ${point.x} in, Calls: ${point.y}`;
                                }
                            }
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            title: {
                                display: true,
                                text: 'Number of 311 Calls',
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
                            title: {
                                display: true,
                                text: 'Precipitation (inches)',
                                font: {
                                    size: 10
                                }
                            },
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
    
    // Weather findings analysis
    updateWeatherFindings();
}

/**
 * Update weather findings analysis
 */
function updateWeatherFindings() {
    const weatherFindings = document.getElementById('weather-findings');
    
    // Analyze weather-sensitive complaints
    if (!processedData.calls311.complaintTypes || Object.keys(processedData.weather).length === 0) {
        weatherFindings.innerHTML = '<p class="text-center p-4">Insufficient data to determine weather correlations.</p>';
        return;
    }
    
    // Group complaints by temperature ranges
    const tempRanges = [
        { name: 'Cold', min: -20, max: 40 },
        { name: 'Cool', min: 40, max: 60 },
        { name: 'Mild', min: 60, max: 75 },
        { name: 'Warm', min: 75, max: 85 },
        { name: 'Hot', min: 85, max: 110 }
    ];
    
    const complaintsTemp = {};
    const complaintsPrec = { 'Rainy': {}, 'Dry': {} };
    
    // Analyze by temperature
    Object.entries(processedData.weather).forEach(([date, weather]) => {
        if (!processedData.calls311.dailyData || !processedData.calls311.dailyData[date]) return;
        
        // Process by temperature
        if (weather.tmax !== null) {
            const range = tempRanges.find(r => weather.tmax >= r.min && weather.tmax < r.max);
            if (!range) return;
            
            // Get complaints for this day
            const complaints = {};
            
            // Filter 311 data for this date
            all311Data.forEach(call => {
                if (call.parsed_date === date) {
                    if (!complaints[call.complaint]) complaints[call.complaint] = 0;
                    complaints[call.complaint]++;
                }
            });
            
            // Aggregate by temperature range
            Object.entries(complaints).forEach(([complaint, count]) => {
                if (!complaintsTemp[complaint]) {
                    complaintsTemp[complaint] = tempRanges.reduce((acc, r) => {
                        acc[r.name] = { count: 0, days: 0 };
                        return acc;
                    }, {});
                }
                
                complaintsTemp[complaint][range.name].count += count;
                complaintsTemp[complaint][range.name].days += 1;
            });
        }
        
        // Process by precipitation
        if (weather.prcp !== null) {
            const isRainy = weather.prcp > 0.1;
            const category = isRainy ? 'Rainy' : 'Dry';
            
            // Filter 311 data for this date
            all311Data.forEach(call => {
                if (call.parsed_date === date) {
                    if (!complaintsPrec[category][call.complaint]) {
                        complaintsPrec[category][call.complaint] = 0;
                    }
                    complaintsPrec[category][call.complaint]++;
                }
            });
        }
    });
    
    // Find temperature-sensitive complaints
    const tempSensitive = Object.entries(complaintsTemp)
        .map(([complaint, ranges]) => {
            // Calculate frequencies
            const frequencies = Object.entries(ranges).map(([range, data]) => {
                return { range, freq: data.days > 0 ? data.count / data.days : 0 };
            });
            
            // Find min and max frequency
            const freqValues = frequencies.map(f => f.freq);
            const minFreq = Math.min(...freqValues);
            const maxFreq = Math.max(...freqValues);
            
            // Calculate spread
            const spread = minFreq > 0 ? maxFreq / minFreq : 0;
            
            return {
                complaint,
                spread,
                frequencies
            };
        })
        .filter(c => c.spread > 1.5 && c.frequencies.some(f => f.freq > 5))
        .sort((a, b) => b.spread - a.spread)
        .slice(0, 5);
    
    // Find precipitation-sensitive complaints
    const precipSensitive = [];
    
    Object.keys(complaintsPrec.Rainy).forEach(complaint => {
        if (complaintsPrec.Dry[complaint]) {
            const rainyCount = complaintsPrec.Rainy[complaint];
            const dryCount = complaintsPrec.Dry[complaint];
            
            // Count rainy and dry days
            const rainyDays = Object.values(processedData.weather)
                .filter(data => data.prcp !== null && data.prcp > 0.1)
                .length;
                
            const dryDays = Object.values(processedData.weather)
                .filter(data => data.prcp !== null && data.prcp <= 0.1)
                .length;
            
            // Calculate rates
            const rainyRate = rainyDays > 0 ? rainyCount / rainyDays : 0;
            const dryRate = dryDays > 0 ? dryCount / dryDays : 0;
            
            // Calculate ratio
            const ratio = Math.max(rainyRate, dryRate) / Math.max(0.1, Math.min(rainyRate, dryRate));
            const diff = Math.abs(rainyRate - dryRate);
            
            if (ratio > 1.3 && diff > 1) {
                precipSensitive.push({
                    complaint,
                    rainyRate,
                    dryRate,
                    ratio,
                    more: rainyRate > dryRate ? 'rainy' : 'dry'
                });
            }
        }
    });
    
    // Sort by ratio
    precipSensitive.sort((a, b) => b.ratio - a.ratio);
    
    // Create HTML output
    let html = '<div class="grid grid-cols-1 md:grid-cols-2 gap-3 text-xs">';
    
    // Temperature findings
    html += '<div>';
    html += '<h4 class="font-medium mb-1">Temperature-Sensitive Complaints:</h4>';
    if (tempSensitive.length === 0) {
        html += '<p>Insufficient data to determine temperature correlation.</p>';
    } else {
        html += '<ul class="list-disc pl-4">';
        tempSensitive.forEach(item => {
            const maxFreq = Math.max(...item.frequencies.map(f => f.freq));
            const maxRange = item.frequencies.find(f => f.freq === maxFreq).range;
            
            html += `<li><span class="font-medium">${item.complaint}</span>: ${Math.round(item.spread * 10) / 10}x more frequent during <span class="font-medium">${maxRange}</span> weather</li>`;
        });
        html += '</ul>';
    }
    html += '</div>';
    
    // Precipitation findings
    html += '<div>';
    html += '<h4 class="font-medium mb-1">Precipitation-Sensitive Complaints:</h4>';
    if (precipSensitive.length === 0) {
        html += '<p>Insufficient data to determine precipitation correlation.</p>';
    } else {
        html += '<ul class="list-disc pl-4">';
        precipSensitive.slice(0, 5).forEach(item => {
            html += `<li><span class="font-medium">${item.complaint}</span>: ${Math.round(item.ratio * 10) / 10}x more frequent during <span class="font-medium">${item.more}</span> days</li>`;
        });
        html += '</ul>';
    }
    html += '</div>';
    
    html += '</div>';
    
    weatherFindings.innerHTML = html;
}