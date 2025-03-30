/**
 * Timeline controls functionality for NYC Urban Rhythm
 */

// Current timeline state
let timelineState = {
    mode: 'hourly', // hourly, daily, weekly, monthly
    timeMarks: [],
    selectedTimeIndex: null
};

/**
 * Initialize timeline controls
 */
function initializeTimelineControls() {
    // Set up view mode buttons
    document.getElementById('hourly-view').addEventListener('click', () => setTimelineMode('hourly'));
    document.getElementById('daily-view').addEventListener('click', () => setTimelineMode('daily'));
    document.getElementById('weekly-view').addEventListener('click', () => setTimelineMode('weekly'));
    document.getElementById('monthly-view').addEventListener('click', () => setTimelineMode('monthly'));
}

/**
 * Set timeline mode
 * @param {string} mode - Timeline mode (hourly, daily, weekly, monthly)
 */
function setTimelineMode(mode) {
    // Update active button
    const buttons = ['hourly-view', 'daily-view', 'weekly-view', 'monthly-view'];
    buttons.forEach(id => {
        const button = document.getElementById(id);
        button.className = button.id === `${mode}-view` 
            ? 'px-2 py-1 text-xs bg-blue-100 rounded' 
            : 'px-2 py-1 text-xs bg-gray-200 rounded';
    });
    
    // Update state
    timelineState.mode = mode;
    
    // Update timeline
    updateTimeline();
}

/**
 * Update timeline based on processed data and current mode
 */
function updateTimeline() {
    const container = document.getElementById('timeline-slider-container');
    container.innerHTML = '';
    
    // Add time axis
    const axis = document.createElement('div');
    axis.className = 'time-axis';
    container.appendChild(axis);
    
    // Get data based on current mode
    const startDate = new Date(document.getElementById('startDate').value);
    const endDate = new Date(document.getElementById('endDate').value);
    
    // Generate time marks based on mode
    timelineState.timeMarks = generateTimeMarks(timelineState.mode, startDate, endDate);
    
    // Create time marks
    timelineState.timeMarks.forEach((mark, index) => {
        // Create mark element
        const markElement = document.createElement('div');
        markElement.className = 'time-mark';
        markElement.style.height = '8px';
        
        // Position based on index
        const position = (index / (timelineState.timeMarks.length - 1)) * 100;
        markElement.style.left = `${position}%`;
        
        // Set intensity based on activity level
        let intensity = 0;
        
        // Sum activity across datasets
        if (processedData.calls311.dailyData && processedData.calls311.dailyData[mark.date]) {
            intensity += processedData.calls311.dailyData[mark.date].total;
        }
        
        if (processedData.transit.dailyData && processedData.transit.dailyData[mark.date]) {
            // Scale down transit numbers to be comparable to 311
            intensity += processedData.transit.dailyData[mark.date].total * 0.1;
        }
        
        if (processedData.taxi.dailyData && processedData.taxi.dailyData[mark.date]) {
            intensity += processedData.taxi.dailyData[mark.date].total;
        }
        
        // Normalize intensity
        const normalizedIntensity = Math.min(Math.log10(intensity + 1) / 4, 1);
        markElement.style.height = `${8 + (normalizedIntensity * 12)}px`;
        
        // Set up click handler
        markElement.addEventListener('click', () => selectTimeMark(index));
        
        // Add tooltip
        markElement.title = mark.label;
        
        // Add to container
        container.appendChild(markElement);
        
        // Add label to select marks
        if (index % Math.max(1, Math.floor(timelineState.timeMarks.length / 10)) === 0 || 
            index === timelineState.timeMarks.length - 1) {
            const label = document.createElement('div');
            label.className = 'time-label';
            label.textContent = mark.label;
            label.style.left = `${position}%`;
            container.appendChild(label);
        }
    });
    
    // Select first time mark by default
    if (timelineState.timeMarks.length > 0 && timelineState.selectedTimeIndex === null) {
        selectTimeMark(0);
    }
}

/**
 * Generate time marks based on mode and date range
 * @param {string} mode - Timeline mode
 * @param {Date} startDate - Start date
 * @param {Date} endDate - End date
 * @returns {Array} - Array of time marks
 */
function generateTimeMarks(mode, startDate, endDate) {
    const marks = [];
    
    switch (mode) {
        case 'hourly':
            // For hourly view, we'll show one day with 24 hours
            const currentDate = new Date(startDate);
            
            for (let hour = 0; hour < 24; hour++) {
                currentDate.setHours(hour, 0, 0, 0);
                
                marks.push({
                    date: formatDate(currentDate),
                    hour: hour,
                    label: `${hour}:00`,
                    timestamp: currentDate.getTime()
                });
            }
            break;
            
        case 'daily':
            // For daily view, we'll show days in the range
            const daily = new Date(startDate);
            
            while (daily <= endDate) {
                marks.push({
                    date: formatDate(daily),
                    label: daily.toLocaleDateString(undefined, { month: 'short', day: 'numeric' }),
                    timestamp: daily.getTime()
                });
                
                daily.setDate(daily.getDate() + 1);
            }
            break;
            
        case 'weekly':
            // For weekly view, we'll show weeks in the range
            const weekly = new Date(startDate);
            
            // Move to the start of the week (Sunday)
            const dayOfWeek = weekly.getDay();
            weekly.setDate(weekly.getDate() - dayOfWeek);
            
            while (weekly <= endDate) {
                const weekEnd = new Date(weekly);
                weekEnd.setDate(weekEnd.getDate() + 6);
                
                marks.push({
                    date: formatDate(weekly),
                    endDate: formatDate(weekEnd),
                    label: `${weekly.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })} - ${weekEnd.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })}`,
                    timestamp: weekly.getTime()
                });
                
                weekly.setDate(weekly.getDate() + 7);
            }
            break;
            
        case 'monthly':
            // For monthly view, we'll show months in the range
            const monthly = new Date(startDate);
            
            // Move to the start of the month
            monthly.setDate(1);
            
            while (monthly <= endDate) {
                marks.push({
                    date: `${monthly.getFullYear()}-${String(monthly.getMonth() + 1).padStart(2, '0')}-01`,
                    label: monthly.toLocaleDateString(undefined, { month: 'long', year: 'numeric' }),
                    timestamp: monthly.getTime()
                });
                
                monthly.setMonth(monthly.getMonth() + 1);
            }
            break;
    }
    
    return marks;
}

/**
 * Select a time mark
 * @param {number} index - Index of time mark to select
 */
function selectTimeMark(index) {
    // Validate index
    if (index < 0 || index >= timelineState.timeMarks.length) return;
    
    // Update state
    timelineState.selectedTimeIndex = index;
    
    // Update UI
    const container = document.getElementById('timeline-slider-container');
    const marks = container.querySelectorAll('.time-mark');
    
    marks.forEach((mark, i) => {
        mark.classList.toggle('selected', i === index);
    });
    
    // Trigger time-based filtering
    updateTimeBasedVisualization(timelineState.timeMarks[index]);
}

/**
 * Update visualizations based on selected time
 * @param {Object} timeMark - Selected time mark
 */
function updateTimeBasedVisualization(timeMark) {
    // Filter data based on selected time
    let filteredData;
    
    switch (timelineState.mode) {
        case 'hourly':
            // Filter for specific hour on specific date
            filteredData = filterDataForHour(timeMark.date, timeMark.hour);
            break;
            
        case 'daily':
            // Filter for specific date
            filteredData = filterDataForDay(timeMark.date);
            break;
            
        case 'weekly':
            // Filter for date range (week)
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
    if (filteredData) {
        updateTimeFilteredVisualization(filteredData, timeMark);
    }
}

/**
 * Filter data for specific hour on specific date
 * @param {string} date - Date in YYYY-MM-DD format
 * @param {number} hour - Hour (0-23)
 * @returns {Object} - Filtered data
 */
function filterDataForHour(date, hour) {
    return {
        calls311: all311Data.filter(item => 
            item.parsed_date === date && item.hour === hour
        ),
        transit: transitData.filter(item => 
            item.parsed_date === date && item.hour === hour
        ),
        taxi: taxiData.filter(item => 
            item.parsed_date === date && item.hour === hour
        ),
        events: eventsData.filter(item => {
            if (item.parsed_date !== date) return false;
            
            // Check if event is active during this hour
            const startHour = new Date(item.start_datetime).getHours();
            const endHour = new Date(item.end_datetime).getHours();
            
            return hour >= startHour && hour <= endHour;
        })
    };
}

/**
 * Filter data for specific date
 * @param {string} date - Date in YYYY-MM-DD format
 * @returns {Object} - Filtered data
 */
function filterDataForDay(date) {
    return {
        calls311: all311Data.filter(item => item.parsed_date === date),
        transit: transitData.filter(item => item.parsed_date === date),
        taxi: taxiData.filter(item => item.parsed_date === date),
        events: eventsData.filter(item => item.parsed_date === date)
    };
}

/**
 * Filter data for date range
 * @param {string} startDate - Start date in YYYY-MM-DD format
 * @param {string} endDate - End date in YYYY-MM-DD format
 * @returns {Object} - Filtered data
 */
function filterDataForDateRange(startDate, endDate) {
    return {
        calls311: all311Data.filter(item => 
            item.parsed_date >= startDate && item.parsed_date <= endDate
        ),
        transit: transitData.filter(item => 
            item.parsed_date >= startDate && item.parsed_date <= endDate
        ),
        taxi: taxiData.filter(item => 
            item.parsed_date >= startDate && item.parsed_date <= endDate
        ),
        events: eventsData.filter(item => {
            const itemDate = new Date(item.parsed_date);
            const start = new Date(startDate);
            const end = new Date(endDate);
            
            return itemDate >= start && itemDate <= end;
        })
    };
}

/**
 * Update visualizations with time-filtered data
 * @param {Object} filteredData - Filtered data
 * @param {Object} timeMark - Selected time mark
 */
function updateTimeFilteredVisualization(filteredData, timeMark) {
    // Process filtered data
    const tempProcessed = {
        calls311: {},
        transit: {},
        taxi: {},
        events: {},
        weather: processedData.weather // Keep existing weather data
    };
    
    // Process each dataset
    if (filteredData.calls311.length > 0) {
        process311Data(filteredData.calls311);
        tempProcessed.calls311 = processedData.calls311;
    }
    
    if (filteredData.transit.length > 0) {
        processTransitData(filteredData.transit);
        tempProcessed.transit = processedData.transit;
    }
    
    if (filteredData.taxi.length > 0) {
        processTaxiData(filteredData.taxi);
        tempProcessed.taxi = processedData.taxi;
    }
    
    if (filteredData.events.length > 0) {
        processEventsData(filteredData.events);
        tempProcessed.events = processedData.events;
    }
    
    // Store original processed data
    const originalProcessed = processedData;
    
    // Set processed data to filtered data
    processedData = tempProcessed;
    
    // Update map visualization
    updateMapVisualization();
    
    // Update info text
    updateTimeFilteredInfoText(filteredData, timeMark);
    
    // Restore original processed data
    processedData = originalProcessed;
}

/**
 * Update info text for time-filtered visualization
 * @param {Object} filteredData - Filtered data
 * @param {Object} timeMark - Selected time mark
 */
function updateTimeFilteredInfoText(filteredData, timeMark) {
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
    
    // Update info text
    updateInfoText(`Showing data for ${timeText}`);
}