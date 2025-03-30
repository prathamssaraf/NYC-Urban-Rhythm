/**
 * Event analysis functionality for NYC Urban Rhythm
 */

/**
 * Analyze event impact on urban activity
 * @param {Array} events - Event data
 * @param {string} startDate - Start date in YYYY-MM-DD format
 * @param {string} endDate - End date in YYYY-MM-DD format
 * @returns {Object} - Event impact analysis
 */
function analyzeEventImpact(events, startDate, endDate) {
    // Filter events that fall within date range
    const filteredEvents = events.filter(event => {
        const eventStart = new Date(event.start_datetime);
        const eventEnd = new Date(event.end_datetime);
        const rangeStart = new Date(startDate);
        const rangeEnd = new Date(endDate);
        
        // Check if event overlaps with date range
        return (eventStart <= rangeEnd && eventEnd >= rangeStart);
    });
    
    // Initialize analysis object
    const analysis = {
        eventCount: filteredEvents.length,
        byBorough: {},
        byType: {},
        impact: []
    };
    
    // Get valid boroughs
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Initialize borough and type counts
    validBoroughs.forEach(borough => {
        analysis.byBorough[borough] = 0;
    });
    
    // Count events by borough and type
    filteredEvents.forEach(event => {
        // Count by borough
        if (analysis.byBorough[event.borough] !== undefined) {
            analysis.byBorough[event.borough]++;
        }
        
        // Count by type
        const type = event.event_type || 'Unknown';
        if (!analysis.byType[type]) {
            analysis.byType[type] = 0;
        }
        analysis.byType[type]++;
    });
    
    // Analyze impact of each event on urban activity
    filteredEvents.forEach(event => {
        const eventDate = formatDate(new Date(event.start_datetime));
        const eventBorough = event.borough;
        
        // Get day before and after event
        const eventDay = new Date(event.start_datetime);
        const dayBefore = formatDate(new Date(eventDay.getTime() - 86400000)); // 1 day in milliseconds
        const dayAfter = formatDate(new Date(eventDay.getTime() + 86400000));
        
        // Check if we have data for all three days
        if (processedData.calls311.dailyData && 
            processedData.calls311.dailyData[dayBefore] && 
            processedData.calls311.dailyData[eventDate] && 
            processedData.calls311.dailyData[dayAfter]) {
            
            // Calculate impact on 311 calls
            const beforeCalls = processedData.calls311.dailyData[dayBefore].byBorough[eventBorough] || 0;
            const duringCalls = processedData.calls311.dailyData[eventDate].byBorough[eventBorough] || 0;
            const afterCalls = processedData.calls311.dailyData[dayAfter].byBorough[eventBorough] || 0;
            
            // Only include if there were calls on the before day (avoid divide by zero)
            if (beforeCalls > 0) {
                const percentChange = ((duringCalls - beforeCalls) / beforeCalls) * 100;
                
                // Check for significant change
                if (Math.abs(percentChange) > 10) {
                    analysis.impact.push({
                        event: event.event_name,
                        date: eventDate,
                        borough: eventBorough,
                        percentChange,
                        beforeCalls,
                        duringCalls,
                        afterCalls,
                        lat: event.lat,
                        lng: event.lng
                    });
                }
            }
        }
        
        // Check impact on transit ridership
        if (processedData.transit.dailyData && 
            processedData.transit.dailyData[dayBefore] && 
            processedData.transit.dailyData[eventDate] && 
            processedData.transit.dailyData[dayAfter]) {
            
            // Calculate transit impact
            const beforeRidership = processedData.transit.dailyData[dayBefore].byBorough[eventBorough] || 0;
            const duringRidership = processedData.transit.dailyData[eventDate].byBorough[eventBorough] || 0;
            const afterRidership = processedData.transit.dailyData[dayAfter].byBorough[eventBorough] || 0;
            
            // Only include if there was ridership on the before day
            if (beforeRidership > 0) {
                const percentChange = ((duringRidership - beforeRidership) / beforeRidership) * 100;
                
                // Check for significant change
                if (Math.abs(percentChange) > 10) {
                    // Add transit impact
                    const existingImpact = analysis.impact.find(i => 
                        i.event === event.event_name && i.date === eventDate
                    );
                    
                    if (existingImpact) {
                        existingImpact.transitChange = percentChange;
                        existingImpact.beforeRidership = beforeRidership;
                        existingImpact.duringRidership = duringRidership;
                    } else {
                        analysis.impact.push({
                            event: event.event_name,
                            date: eventDate,
                            borough: eventBorough,
                            transitChange: percentChange,
                            beforeRidership,
                            duringRidership,
                            afterRidership,
                            lat: event.lat,
                            lng: event.lng
                        });
                    }
                }
            }
        }
        
        // Check impact on taxi pickups
        if (processedData.taxi.dailyData && 
            processedData.taxi.dailyData[dayBefore] && 
            processedData.taxi.dailyData[eventDate] && 
            processedData.taxi.dailyData[dayAfter]) {
            
            // Calculate taxi impact
            const beforePickups = processedData.taxi.dailyData[dayBefore].byBorough[eventBorough] || 0;
            const duringPickups = processedData.taxi.dailyData[eventDate].byBorough[eventBorough] || 0;
            const afterPickups = processedData.taxi.dailyData[dayAfter].byBorough[eventBorough] || 0;
            
            // Only include if there were pickups on the before day
            if (beforePickups > 0) {
                const percentChange = ((duringPickups - beforePickups) / beforePickups) * 100;
                
                // Check for significant change
                if (Math.abs(percentChange) > 10) {
                    // Add taxi impact
                    const existingImpact = analysis.impact.find(i => 
                        i.event === event.event_name && i.date === eventDate
                    );
                    
                    if (existingImpact) {
                        existingImpact.taxiChange = percentChange;
                        existingImpact.beforePickups = beforePickups;
                        existingImpact.duringPickups = duringPickups;
                    } else {
                        analysis.impact.push({
                            event: event.event_name,
                            date: eventDate,
                            borough: eventBorough,
                            taxiChange: percentChange,
                            beforePickups,
                            duringPickups,
                            afterPickups,
                            lat: event.lat,
                            lng: event.lng
                        });
                    }
                }
            }
        }
    });
    
    // Sort impact by magnitude
    analysis.impact.sort((a, b) => {
        const aMax = Math.max(
            Math.abs(a.percentChange || 0),
            Math.abs(a.transitChange || 0),
            Math.abs(a.taxiChange || 0)
        );
        
        const bMax = Math.max(
            Math.abs(b.percentChange || 0),
            Math.abs(b.transitChange || 0),
            Math.abs(b.taxiChange || 0)
        );
        
        return bMax - aMax;
    });
    
    return analysis;
}

/**
 * Find nearby activity for an event
 * @param {Object} event - Event data
 * @param {number} radiusMeters - Radius in meters
 * @returns {Object} - Nearby activity counts
 */
function findNearbyActivity(event, radiusMeters = 500) {
    // Check if event has coordinates
    if (!event.lat || !event.lng) return null;
    
    const activity = {
        calls311: 0,
        transit: 0,
        taxi: 0
    };
    
    // Find nearby 311 calls
    if (all311Data && all311Data.length > 0) {
        activity.calls311 = all311Data.filter(call => {
            // Check if call is on the event date
            if (call.parsed_date !== formatDate(new Date(event.start_datetime))) return false;
            
            // Check if call has coordinates
            if (!call.lat || !call.lng) return false;
            
            // Calculate distance
            const distance = calculateDistance(
                event.lat, event.lng,
                call.lat, call.lng
            );
            
            return distance <= radiusMeters;
        }).length;
    }
    
    // Find nearby transit stations
    if (processedData.transit.stationRidership) {
        Object.values(processedData.transit.stationRidership).forEach(station => {
            if (!station.lat || !station.lng) return;
            
            // Calculate distance
            const distance = calculateDistance(
                event.lat, event.lng,
                station.lat, station.lng
            );
            
            if (distance <= radiusMeters) {
                // Add station ridership for event date
                const eventDate = formatDate(new Date(event.start_datetime));
                
                // Find hourly data for this station on event date
                transitData.forEach(entry => {
                    if (entry.parsed_date === eventDate && 
                        entry.station_complex_id === station.id) {
                        activity.transit += parseFloat(entry.ridership) || 0;
                    }
                });
            }
        });
    }
    
    // Find nearby taxi pickups
    if (taxiData && taxiData.length > 0) {
        // We don't have exact pickup coordinates, so use borough-level data
        const eventDate = formatDate(new Date(event.start_datetime));
        
        if (processedData.taxi.dailyData && processedData.taxi.dailyData[eventDate]) {
            activity.taxi = processedData.taxi.dailyData[eventDate].byBorough[event.borough] || 0;
        }
    }
    
    return activity;
}

/**
 * Generate event impact visualization
 * @param {Array} events - Events to visualize
 * @param {string} startDate - Start date
 * @param {string} endDate - End date
 * @returns {Object} - Visualization data
 */
function generateEventVisualization(events, startDate, endDate) {
    // Analyze event impact
    const impact = analyzeEventImpact(events, startDate, endDate);
    
    // Generate visualization data
    const visualization = {
        eventMarkers: [],
        impactAreas: [],
        stats: {
            totalEvents: impact.eventCount,
            significantImpact: impact.impact.length,
            byBorough: impact.byBorough,
            byType: impact.byType
        }
    };
    
    // Create markers for all events
    events.forEach(event => {
        if (!event.lat || !event.lng) return;
        
        // Format date for display
        const eventDate = new Date(event.start_datetime);
        const formattedDate = eventDate.toLocaleDateString();
        
        // Create marker data
        visualization.eventMarkers.push({
            lat: event.lat,
            lng: event.lng,
            name: event.event_name,
            date: formattedDate,
            borough: event.borough,
            type: event.event_type || 'Unknown',
            popup: `
                <div class="font-medium text-sm">${event.event_name}</div>
                <div class="text-xs">${formattedDate}</div>
                <div class="text-xs">${event.borough}</div>
                <div class="text-xs">${event.event_type || 'No type specified'}</div>
            `
        });
    });
    
    // Create impact areas for significant events
    impact.impact.forEach(event => {
        if (!event.lat || !event.lng) return;
        
        // Calculate overall impact score
        const impactScore = Math.max(
            Math.abs(event.percentChange || 0),
            Math.abs(event.transitChange || 0),
            Math.abs(event.taxiChange || 0)
        );
        
        // Scale radius based on impact score
        const radius = Math.min(500 + (impactScore * 10), 2000);
        
        // Create impact area data
        visualization.impactAreas.push({
            lat: event.lat,
            lng: event.lng,
            radius,
            event: event.event,
            date: event.date,
            borough: event.borough,
            callsChange: event.percentChange,
            transitChange: event.transitChange,
            taxiChange: event.taxiChange,
            color: impactScore > 50 ? '#EF4444' : (impactScore > 25 ? '#F59E0B' : '#10B981'),
            popup: `
                <div class="font-medium text-sm">${event.event}</div>
                <div class="text-xs">${event.date} - ${event.borough}</div>
                ${event.percentChange ? `<div class="text-xs">311 Calls: ${event.percentChange.toFixed(1)}%</div>` : ''}
                ${event.transitChange ? `<div class="text-xs">Transit: ${event.transitChange.toFixed(1)}%</div>` : ''}
                ${event.taxiChange ? `<div class="text-xs">Taxi: ${event.taxiChange.toFixed(1)}%</div>` : ''}
            `
        });
    });
    
    return visualization;
}

/**
 * Update event list display
 * @param {Array} events - Events to display
 */
function updateEventList(events) {
    const eventsList = document.getElementById('events-list');
    eventsList.innerHTML = '';
    
    if (!events || events.length === 0) {
        eventsList.innerHTML = '<p class="text-center p-4">No events found in the selected date range.</p>';
        return;
    }
    
    // Sort events by date
    const sortedEvents = [...events].sort((a, b) => new Date(a.start_datetime) - new Date(b.start_datetime));
    
    // Display top events
    sortedEvents.slice(0, 10).forEach(event => {
        const div = document.createElement('div');
        div.className = 'p-2 border-b';
        
        const startDate = new Date(event.start_datetime);
        const formattedDate = startDate.toLocaleDateString();
        
        div.innerHTML = `
            <div class="font-medium text-sm">${event.event_name}</div>
            <div class="text-xs flex justify-between mt-1">
                <span>${formattedDate}</span>
                <span class="font-medium">${event.borough}</span>
            </div>
        `;
        
        // Add click handler to show event on map
        div.addEventListener('click', () => {
            if (event.lat && event.lng) {
                // Switch to map tab
                document.getElementById('tab-map').click();
                
                // Fly to event location
                map.flyTo({
                    center: [event.lng, event.lat],
                    zoom: 14,
                    duration: 1000
                });
                
                // Create pulse effect
                createPulseEffect(map, [event.lng, event.lat], DATASET_COLORS.events);
            }
        });
        
        eventsList.appendChild(div);
    });
}