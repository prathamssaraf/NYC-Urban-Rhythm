/**
 * Main application logic for NYC Urban Rhythm
 */

// Initialize application on document load
document.addEventListener('DOMContentLoaded', () => {
    // Initialize components
    initializeMap();
    initializeDataLoaders();
    initializeCharts();
    initializeTimelineControls();
    
    // Set up update button handler
    document.getElementById('updateDashboardBtn').addEventListener('click', updateDashboard);
    
    // Set up tab switching
    setupTabs();
});

/**
 * Set up tab switching
 */
function setupTabs() {
    const tabs = {
        map: document.getElementById('tab-map'),
        trends: document.getElementById('tab-trends'),
        categories: document.getElementById('tab-categories'),
        transit: document.getElementById('tab-transit'),
        taxi: document.getElementById('tab-taxi'),
        events: document.getElementById('tab-events'),
        correlation: document.getElementById('tab-correlation'),
        weather: document.getElementById('tab-weather')
    };
    
    const contents = {
        map: document.getElementById('content-map'),
        trends: document.getElementById('content-trends'),
        categories: document.getElementById('content-categories'),
        transit: document.getElementById('content-transit'),
        taxi: document.getElementById('content-taxi'),
        events: document.getElementById('content-events'),
        correlation: document.getElementById('content-correlation'),
        weather: document.getElementById('content-weather')
    };
    
    // Set up tab click handlers
    Object.keys(tabs).forEach(tabKey => {
        tabs[tabKey].addEventListener('click', () => {
            // Update active tab
            Object.values(tabs).forEach(tab => {
                tab.classList.remove('tab-active');
            });
            tabs[tabKey].classList.add('tab-active');
            
            // Update visible content
            Object.values(contents).forEach(content => {
                content.classList.add('hidden');
            });
            contents[tabKey].classList.remove('hidden');
            
            // Special handling for map tab
            if (tabKey === 'map') {
                // Trigger map resize to fix rendering issues
                map.resize();
            }
        });
    });
}

/**
 * Update dashboard with current data
 */
async function updateDashboard() {
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;
    
    // Validate date range
    if (!startDate || !endDate || startDate > endDate) {
        updateInfoText('Invalid date range.');
        return;
    }
    
    // Disable controls during update
    document.getElementById('startDate').disabled = true;
    document.getElementById('endDate').disabled = true;
    document.getElementById('updateDashboardBtn').disabled = true;
    
    // Show loading indicator
    showLoading('Processing data & updating visualizations...');
    
    try {
        // Get datasets filtered by date range
        const filteredData = getFilteredDatasets(startDate, endDate);
        
        // Process all data
        processAllData(startDate, endDate, filteredData);
        
        // Fetch and add weather data if possible
        try {
            const weatherData = await fetchWeatherData(startDate, endDate);
            processedData.weather = weatherData;
        } catch (e) {
            console.error('Error fetching weather data:', e);
            processedData.weather = {};
        }
        
        // Calculate correlations
        processedData.correlations = {
            transitVs311: {},
            taxiVs311: {},
            eventsImpact: []
        };
        
        // Update visualizations
        updateMapVisualization();
        updateAllCharts();
        
        // Update stats summary
        const stats = getDatasetStats(filteredData);
        updateStatsSummary(stats);
        
        // Calculate and store correlations
        const correlations = calculateAllCorrelations();
        
        // Update correlation dashboard
        updateCorrelationDashboard(correlations);
        
        // Update info text
        updateInfoText(`Dashboard updated with data from ${startDate} to ${endDate}.`);
    } catch (error) {
        console.error('Error updating dashboard:', error);
        updateInfoText('Error updating dashboard. Please try again.');
    } finally {
        // Hide loading indicator
        hideLoading();
        
        // Re-enable controls
        document.getElementById('startDate').disabled = false;
        document.getElementById('endDate').disabled = false;
        document.getElementById('updateDashboardBtn').disabled = false;
    }
}

/**
 * Update time-filtered visualizations based on current timeline state
 */
function updateTimeBasedVisualizations() {
    const timeMark = timelineState.timeMarks[timelineState.selectedTimeIndex];
    if (!timeMark) return;
    
    // Create appropriate filter based on timeline mode
    let filteredData;
    
    switch (timelineState.mode) {
        case 'hourly':
            // Filter for specific hour
            filteredData = filterDataForHour(timeMark.date, timeMark.hour);
            break;
            
        case 'daily':
            // Filter for specific date
            filteredData = filterDataForDay(timeMark.date);
            break;
            
        case 'weekly':
            // Filter for week range
            filteredData = filterDataForDateRange(timeMark.date, timeMark.endDate);
            break;
            
        case 'monthly':
            // Filter for month
            const year = parseInt(timeMark.date.substring(0, 4));
            const month = parseInt(timeMark.date.substring(5, 7)) - 1;
            const startDate = new Date(year, month, 1);
            const endDate = new Date(year, month + 1, 0);
            
            filteredData = filterDataForDateRange(
                formatDate(startDate),
                formatDate(endDate)
            );
            break;
    }
    
    // Update visualizations with filtered data
    updateTimeFilteredVisualizations(filteredData, timeMark);
}

/**
 * Update visualizations with time-filtered data
 * @param {Object} filteredData - Filtered data 
 * @param {Object} timeMark - Current time mark
 */
function updateTimeFilteredVisualizations(filteredData, timeMark) {
    // Process filtered data
    const tempProcessed = {
        calls311: {},
        transit: {},
        taxi: {},
        events: {},
        weather: processedData.weather
    };
    
    // Save original processed data
    const originalProcessed = processedData;
    
    // Process 311 data
    if (filteredData.calls311.length > 0) {
        process311Data(filteredData.calls311);
        tempProcessed.calls311 = processedData.calls311;
    }
    
    // Process transit data
    if (filteredData.transit.length > 0) {
        processTransitData(filteredData.transit);
        tempProcessed.transit = processedData.transit;
    }
    
    // Process taxi data
    if (filteredData.taxi.length > 0) {
        processTaxiData(filteredData.taxi);
        tempProcessed.taxi = processedData.taxi;
    }
    
    // Process events data
    if (filteredData.events.length > 0) {
        processEventsData(filteredData.events);
        tempProcessed.events = processedData.events;
    }
    
    // Set processed data to filtered data
    processedData = tempProcessed;
    
    // Update map visualization
    updateMapVisualization();
    
    // Update info text
    let timeText = '';
    
    switch (timelineState.mode) {
        case 'hourly':
            timeText = `${timeMark.date} at ${timeMark.hour}:00`;
            break;
            
        case 'daily':
            timeText = timeMark.date;
            break;
            
        case 'weekly':
            timeText = `week of ${timeMark.date}`;
            break;
            
        case 'monthly':
            timeText = timeMark.label;
            break;
    }
    
    updateInfoText(`Showing data for ${timeText}`);
    
    // Count records in each dataset
    const counts = {
        calls311: filteredData.calls311.length,
        transit: filteredData.transit.reduce((sum, item) => sum + (parseFloat(item.ridership) || 0), 0),
        taxi: filteredData.taxi.length,
        events: filteredData.events.length
    };
    
    // Update stats
    document.getElementById('stat-311').textContent = `311 Calls: ${formatNumber(counts.calls311)}`;
    document.getElementById('stat-transit').textContent = `Subway Entries: ${formatNumber(Math.round(counts.transit))}`;
    document.getElementById('stat-taxi').textContent = `Taxi Trips: ${formatNumber(counts.taxi)}`;
    document.getElementById('stat-events').textContent = `Events: ${formatNumber(counts.events)}`;
    
    // Restore original processed data
    processedData = originalProcessed;
}