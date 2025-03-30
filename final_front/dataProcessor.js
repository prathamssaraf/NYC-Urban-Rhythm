/**
 * Data processing and aggregation for NYC Urban Rhythm
 */

// Processed data storage
let processedData = {
    calls311: {},
    transit: {},
    taxi: {},
    events: {},
    weather: {}
};

/**
 * Process and aggregate all datasets based on date range
 * @param {string} startDate - Start date in YYYY-MM-DD format
 * @param {string} endDate - End date in YYYY-MM-DD format  
 * @param {Object} filteredData - Object containing filtered datasets
 * @returns {Object} - Aggregated data for visualization
 */
function processAllData(startDate, endDate, filteredData) {
    // Reset processed data
    processedData = {
        calls311: {},
        transit: {},
        taxi: {},
        events: {},
        weather: {}
    };
    
    // Process 311 call data
    process311Data(filteredData.calls311);
    
    // Process transit data
    processTransitData(filteredData.transit);
    
    // Process taxi data
    processTaxiData(filteredData.taxi);
    
    // Process events data
    processEventsData(filteredData.events);
    
    return processedData;
}

/**
 * Process 311 call data
 * @param {Array} data - Filtered 311 call data 
 */
function process311Data(data) {
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Aggregate calls by borough
    const aggregatedByBorough = validBoroughs.reduce((acc, borough) => {
        acc[borough] = 0;
        return acc;
    }, {});
    
    // Aggregate by complaint type
    const complaintTypes = {};
    
    // Aggregate by hour
    const hourlyByBorough = validBoroughs.reduce((acc, borough) => {
        acc[borough] = Array(24).fill(0);
        return acc;
    }, {});
    
    // Aggregate by day
    const dailyData = {};
    
    // Aggregate by day of week
    const dowByBorough = validBoroughs.reduce((acc, borough) => {
        acc[borough] = Array(7).fill(0);
        return acc;
    }, {});
    
    // Process each record
    data.forEach(record => {
        const borough = record.borough;
        
        // Increment borough count
        aggregatedByBorough[borough]++;
        
        // Increment complaint type count
        if (!complaintTypes[record.complaint]) {
            complaintTypes[record.complaint] = {
                total: 0,
                byBorough: validBoroughs.reduce((acc, b) => {
                    acc[b] = 0;
                    return acc;
                }, {})
            };
        }
        complaintTypes[record.complaint].total++;
        complaintTypes[record.complaint].byBorough[borough]++;
        
        // Increment hourly count
        hourlyByBorough[borough][record.hour]++;
        
        // Increment day of week count
        dowByBorough[borough][record.dow]++;
        
        // Aggregate by day
        if (!dailyData[record.parsed_date]) {
            dailyData[record.parsed_date] = {
                total: 0,
                byBorough: validBoroughs.reduce((acc, b) => {
                    acc[b] = 0;
                    return acc;
                }, {})
            };
        }
        dailyData[record.parsed_date].total++;
        dailyData[record.parsed_date].byBorough[borough]++;
    });
    
    // Store processed data
    processedData.calls311 = {
        totalByBorough: aggregatedByBorough,
        complaintTypes,
        hourlyByBorough,
        dailyData,
        dowByBorough
    };
}

/**
 * Process transit data
 * @param {Array} data - Filtered transit data
 */
function processTransitData(data) {
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Aggregate ridership by borough
    const ridershipByBorough = validBoroughs.reduce((acc, borough) => {
        acc[borough] = 0;
        return acc;
    }, {});
    
    // Aggregate by station
    const stationRidership = {};
    
    // Aggregate by hour
    const hourlyByBorough = validBoroughs.reduce((acc, borough) => {
        acc[borough] = Array(24).fill(0);
        return acc;
    }, {});
    
    // Aggregate by day
    const dailyData = {};
    
    // Aggregate by day of week
    const dowByBorough = validBoroughs.reduce((acc, borough) => {
        acc[borough] = Array(7).fill(0);
        return acc;
    }, {});
    
    // Aggregate by payment method
    const paymentMethods = {};
    
    // Process each record
    data.forEach(record => {
        const borough = record.borough;
        const ridership = parseFloat(record.ridership) || 0;
        
        // Increment borough ridership
        ridershipByBorough[borough] += ridership;
        
        // Increment station ridership
        if (!stationRidership[record.station_complex]) {
            stationRidership[record.station_complex] = {
                id: record.station_complex_id,
                total: 0,
                byHour: Array(24).fill(0),
                lat: record.lat,
                lng: record.lng,
                borough
            };
        }
        stationRidership[record.station_complex].total += ridership;
        stationRidership[record.station_complex].byHour[record.hour] += ridership;
        
        // Increment hourly count
        hourlyByBorough[borough][record.hour] += ridership;
        
        // Increment day of week count
        dowByBorough[borough][record.dow] += ridership;
        
        // Aggregate by day
        if (!dailyData[record.parsed_date]) {
            dailyData[record.parsed_date] = {
                total: 0,
                byBorough: validBoroughs.reduce((acc, b) => {
                    acc[b] = 0;
                    return acc;
                }, {})
            };
        }
        dailyData[record.parsed_date].total += ridership;
        dailyData[record.parsed_date].byBorough[borough] += ridership;
        
        // Aggregate by payment method
        const method = record.payment_method || 'Unknown';
        if (!paymentMethods[method]) {
            paymentMethods[method] = {
                total: 0,
                byBorough: validBoroughs.reduce((acc, b) => {
                    acc[b] = 0;
                    return acc;
                }, {})
            };
        }
        paymentMethods[method].total += ridership;
        paymentMethods[method].byBorough[borough] += ridership;
    });
    
    // Store processed data
    processedData.transit = {
        ridershipByBorough,
        stationRidership,
        hourlyByBorough,
        dailyData,
        dowByBorough,
        paymentMethods
    };
}

/**
 * Process taxi data
 * @param {Array} data - Filtered taxi data
 */
function processTaxiData(data) {
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Aggregate pickups by borough
    const pickupsByBorough = validBoroughs.reduce((acc, borough) => {
        acc[borough] = 0;
        return acc;
    }, {});
    
    // Aggregate dropoffs by borough
    const dropoffsByBorough = validBoroughs.reduce((acc, borough) => {
        acc[borough] = 0;
        return acc;
    }, {});
    
    // Aggregate by hour
    const hourlyByBorough = validBoroughs.reduce((acc, borough) => {
        acc[borough] = Array(24).fill(0);
        return acc;
    }, {});
    
    // Aggregate by day
    const dailyData = {};
    
    // Aggregate by day of week
    const dowByBorough = validBoroughs.reduce((acc, borough) => {
        acc[borough] = Array(7).fill(0);
        return acc;
    }, {});
    
    // Track distance and fare metrics
    const metrics = {
        avgDistance: 0,
        avgFare: 0,
        avgTip: 0,
        distanceByBorough: validBoroughs.reduce((acc, borough) => {
            acc[borough] = [];
            return acc;
        }, {}),
        fareByBorough: validBoroughs.reduce((acc, borough) => {
            acc[borough] = [];
            return acc;
        }, {})
    };
    
    // Process each record
    data.forEach(record => {
        // Add to pickup borough counts
        if (record.pu_borough && validBoroughs.includes(record.pu_borough)) {
            pickupsByBorough[record.pu_borough]++;
            hourlyByBorough[record.pu_borough][record.hour]++;
            dowByBorough[record.pu_borough][record.dow]++;
            
            // Add to metrics
            metrics.distanceByBorough[record.pu_borough].push(record.trip_distance);
            metrics.fareByBorough[record.pu_borough].push(record.fare_amount);
        }
        
        // Add to dropoff borough counts
        if (record.do_borough && validBoroughs.includes(record.do_borough)) {
            dropoffsByBorough[record.do_borough]++;
        }
        
        // Aggregate by day
        if (!dailyData[record.parsed_date]) {
            dailyData[record.parsed_date] = {
                total: 0,
                byBorough: validBoroughs.reduce((acc, b) => {
                    acc[b] = 0;
                    return acc;
                }, {})
            };
        }
        dailyData[record.parsed_date].total++;
        
        if (record.pu_borough && validBoroughs.includes(record.pu_borough)) {
            dailyData[record.parsed_date].byBorough[record.pu_borough]++;
        }
        
        // Add to overall metrics
        metrics.avgDistance += record.trip_distance;
        metrics.avgFare += record.fare_amount;
        metrics.avgTip += record.tip_amount;
    });
    
    // Calculate averages
    if (data.length > 0) {
        metrics.avgDistance /= data.length;
        metrics.avgFare /= data.length;
        metrics.avgTip /= data.length;
    }
    
    // Calculate borough averages
    validBoroughs.forEach(borough => {
        const distances = metrics.distanceByBorough[borough];
        const fares = metrics.fareByBorough[borough];
        
        metrics.distanceByBorough[borough] = distances.length > 0 
            ? distances.reduce((sum, val) => sum + val, 0) / distances.length 
            : 0;
            
        metrics.fareByBorough[borough] = fares.length > 0 
            ? fares.reduce((sum, val) => sum + val, 0) / fares.length 
            : 0;
    });
    
    // Store processed data
    processedData.taxi = {
        pickupsByBorough,
        dropoffsByBorough,
        hourlyByBorough,
        dailyData,
        dowByBorough,
        metrics
    };
}

/**
 * Process events data
 * @param {Array} data - Filtered events data
 */
function processEventsData(data) {
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Aggregate events by borough
    const eventsByBorough = validBoroughs.reduce((acc, borough) => {
        acc[borough] = 0;
        return acc;
    }, {});
    
    // Aggregate by event type
    const eventTypes = {};
    
    // Aggregate by day
    const dailyData = {};
    
    // Aggregate by day of week
    const dowByBorough = validBoroughs.reduce((acc, borough) => {
        acc[borough] = Array(7).fill(0);
        return acc;
    }, {});
    
    // Aggregate by agency
    const eventAgencies = {};
    
    // Process each record
    data.forEach(record => {
        const borough = record.borough;
        
        // Increment borough count
        eventsByBorough[borough]++;
        
        // Increment event type count
        const type = record.event_type || 'Unknown';
        if (!eventTypes[type]) {
            eventTypes[type] = {
                total: 0,
                byBorough: validBoroughs.reduce((acc, b) => {
                    acc[b] = 0;
                    return acc;
                }, {})
            };
        }
        eventTypes[type].total++;
        eventTypes[type].byBorough[borough]++;
        
        // Increment day of week count
        dowByBorough[borough][record.dow]++;
        
        // Aggregate by day
        if (!dailyData[record.parsed_date]) {
            dailyData[record.parsed_date] = {
                total: 0,
                byBorough: validBoroughs.reduce((acc, b) => {
                    acc[b] = 0;
                    return acc;
                }, {})
            };
        }
        dailyData[record.parsed_date].total++;
        dailyData[record.parsed_date].byBorough[borough]++;
        
        // Aggregate by agency
        const agency = record.event_agency || 'Unknown';
        if (!eventAgencies[agency]) {
            eventAgencies[agency] = {
                total: 0,
                byBorough: validBoroughs.reduce((acc, b) => {
                    acc[b] = 0;
                    return acc;
                }, {})
            };
        }
        eventAgencies[agency].total++;
        eventAgencies[agency].byBorough[borough]++;
    });
    
    // Store processed data
    processedData.events = {
        eventsByBorough,
        eventTypes,
        dailyData,
        dowByBorough,
        eventAgencies,
        eventsList: data.map(event => ({
            id: event.event_id,
            name: event.event_name,
            start: event.start_datetime,
            end: event.end_datetime,
            borough: event.borough,
            location: event.location,
            type: event.event_type,
            lat: event.lat,
            lng: event.lng
        }))
    };
}

/**
 * Calculate correlations between datasets
 * @returns {Object} - Correlation analysis results
 */
function calculateDatasetCorrelations() {
    const correlations = {
        transitVs311: {},
        taxiVs311: {},
        eventsImpact: []
    };
    
    console.log("Data available for correlations:", {
        "311 daily data": processedData.calls311.dailyData ? Object.keys(processedData.calls311.dailyData).length : 0,
        "Transit daily data": processedData.transit.dailyData ? Object.keys(processedData.transit.dailyData).length : 0,
        "Taxi daily data": processedData.taxi.dailyData ? Object.keys(processedData.taxi.dailyData).length : 0
    });
    
    // Get valid boroughs
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);

    
    // Only proceed if we have data for all required datasets
    if (!processedData.calls311.dailyData || 
        !processedData.transit.dailyData ||
        !processedData.taxi.dailyData) {
        return correlations;
    }
    
    // Get all dates that exist in all three datasets
    const dates = Object.keys(processedData.calls311.dailyData).filter(date => 
        processedData.transit.dailyData[date] && 
        processedData.taxi.dailyData[date]
    );
    
    if (dates.length === 0) {
        return correlations;
    }
    
    // // Calculate borough-level correlations
    // const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    validBoroughs.forEach(borough => {
        // Get daily activity for this borough
        const calls311 = dates.map(date => 
            processedData.calls311.dailyData[date].byBorough[borough] || 0
        );
        
        const transit = dates.map(date => 
            processedData.transit.dailyData[date].byBorough[borough] || 0
        );
        
        const taxi = dates.map(date => 
            processedData.taxi.dailyData[date].byBorough[borough] || 0
        );
        
        // Calculate correlations
        correlations.transitVs311[borough] = calculateCorrelation(transit, calls311);
        correlations.taxiVs311[borough] = calculateCorrelation(taxi, calls311);
    });
    
    // Calculate event impact
    // If we have event data, analyze the impact on other activity
    if (processedData.events.eventsList && processedData.events.eventsList.length > 0) {
        const eventImpacts = [];
        
        processedData.events.eventsList.forEach(event => {
            // Get dates before, during, and after event
            const eventDate = new Date(event.start);
            const dayBefore = formatDate(new Date(eventDate.getTime() - 86400000));
            const eventDay = formatDate(eventDate);
            const dayAfter = formatDate(new Date(eventDate.getTime() + 86400000));
            
            // Calculate impact if we have data for all three days
            if (processedData.calls311.dailyData[dayBefore] && 
                processedData.calls311.dailyData[eventDay] && 
                processedData.calls311.dailyData[dayAfter]) {
                
                const borough = event.borough;
                
                // Calculate percent change in 311 calls
                const beforeCalls = processedData.calls311.dailyData[dayBefore].byBorough[borough] || 0;
                const duringCalls = processedData.calls311.dailyData[eventDay].byBorough[borough] || 0;
                const afterCalls = processedData.calls311.dailyData[dayAfter].byBorough[borough] || 0;
                
                // Only include if there were calls on the before day (avoid divide by zero)
                if (beforeCalls > 0) {
                    const percentChange = ((duringCalls - beforeCalls) / beforeCalls) * 100;
                    
                    eventImpacts.push({
                        event: event.name,
                        borough,
                        date: eventDay,
                        percentChange,
                        beforeCalls,
                        duringCalls,
                        afterCalls
                    });
                }
            }
        });
        
        // Sort by absolute impact
        eventImpacts.sort((a, b) => Math.abs(b.percentChange) - Math.abs(a.percentChange));
        
        correlations.eventsImpact = eventImpacts;
    }
    
    // Before returning, log the calculated correlations
    console.log("Calculated correlations:", {
        transitVs311: correlations.transitVs311,
        taxiVs311: correlations.taxiVs311,
        eventsImpactCount: correlations.eventsImpact.length
    });
    
    return correlations;
}

/**
 * Calculate neighborhood rhythm scores
 * @returns {Object} - Rhythm scores by borough and time
 */
function calculateRhythmScores() {
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    const rhythms = {};
    
    validBoroughs.forEach(borough => {
        // Initialize rhythm object for this borough
        rhythms[borough] = {
            hourly: Array(24).fill(0),
            dow: Array(7).fill(0),
            overallActivity: 0,
            peakHour: 0,
            peakDay: 0
        };
        
        // Add 311 calls contribution
        if (processedData.calls311.hourlyByBorough && processedData.calls311.dowByBorough) {
            for (let i = 0; i < 24; i++) {
                rhythms[borough].hourly[i] += processedData.calls311.hourlyByBorough[borough][i] || 0;
            }
            for (let i = 0; i < 7; i++) {
                rhythms[borough].dow[i] += processedData.calls311.dowByBorough[borough][i] || 0;
            }
            rhythms[borough].overallActivity += processedData.calls311.totalByBorough[borough] || 0;
        }
        
        // Add transit contribution
        if (processedData.transit.hourlyByBorough && processedData.transit.dowByBorough) {
            // Transit data has larger numbers, so scale it down
            const transitScale = 0.1;
            for (let i = 0; i < 24; i++) {
                rhythms[borough].hourly[i] += (processedData.transit.hourlyByBorough[borough][i] || 0) * transitScale;
            }
            for (let i = 0; i < 7; i++) {
                rhythms[borough].dow[i] += (processedData.transit.dowByBorough[borough][i] || 0) * transitScale;
            }
            rhythms[borough].overallActivity += (processedData.transit.ridershipByBorough[borough] || 0) * transitScale;
        }
        
        // Add taxi contribution
        if (processedData.taxi.hourlyByBorough && processedData.taxi.dowByBorough) {
            for (let i = 0; i < 24; i++) {
                rhythms[borough].hourly[i] += processedData.taxi.hourlyByBorough[borough][i] || 0;
            }
            for (let i = 0; i < 7; i++) {
                rhythms[borough].dow[i] += processedData.taxi.dowByBorough[borough][i] || 0;
            }
            rhythms[borough].overallActivity += processedData.taxi.pickupsByBorough[borough] || 0;
        }
        
        // Find peak hour and day
        let maxHour = 0, maxHourVal = 0;
        let maxDay = 0, maxDayVal = 0;
        
        for (let i = 0; i < 24; i++) {
            if (rhythms[borough].hourly[i] > maxHourVal) {
                maxHourVal = rhythms[borough].hourly[i];
                maxHour = i;
            }
        }
        
        for (let i = 0; i < 7; i++) {
            if (rhythms[borough].dow[i] > maxDayVal) {
                maxDayVal = rhythms[borough].dow[i];
                maxDay = i;
            }
        }
        
        rhythms[borough].peakHour = maxHour;
        rhythms[borough].peakDay = maxDay;
    });
    
    return rhythms;
}

/**
 * Detect activity clusters across datasets
 * @returns {Array} - Detected activity clusters
 */
function detectActivityClusters() {
    const clusters = [];
    const allPoints = [];
    
    // Collect points from all datasets
    if (processedData.calls311.totalByBorough) {
        all311Data.forEach(point => {
            if (point.lat && point.lng) {
                allPoints.push({
                    lat: point.lat,
                    lng: point.lng,
                    type: 'calls311',
                    hour: point.hour,
                    dow: point.dow,
                    date: point.parsed_date
                });
            }
        });
    }
    
    if (processedData.transit.stationRidership) {
        Object.values(processedData.transit.stationRidership).forEach(station => {
            if (station.lat && station.lng) {
                allPoints.push({
                    lat: station.lat,
                    lng: station.lng,
                    type: 'transit',
                    value: station.total
                });
            }
        });
    }
    
    if (processedData.events.eventsList) {
        processedData.events.eventsList.forEach(event => {
            if (event.lat && event.lng) {
                allPoints.push({
                    lat: event.lat,
                    lng: event.lng,
                    type: 'event',
                    name: event.name,
                    date: event.start
                });
            }
        });
    }
    
    // Skip clustering if we don't have enough points
    if (allPoints.length < 10) return clusters;
    
    // Very basic clustering - group by grid cells
    const gridSize = 0.005; // Roughly 500m at NYC latitude
    const gridCells = {};
    
    allPoints.forEach(point => {
        const gridX = Math.floor(point.lng / gridSize);
        const gridY = Math.floor(point.lat / gridSize);
        const key = `${gridX}:${gridY}`;
        
        if (!gridCells[key]) {
            gridCells[key] = {
                points: [],
                center: {
                    lat: (gridY + 0.5) * gridSize,
                    lng: (gridX + 0.5) * gridSize
                },
                types: {}
            };
        }
        
        gridCells[key].points.push(point);
        
        if (!gridCells[key].types[point.type]) {
            gridCells[key].types[point.type] = 0;
        }
        gridCells[key].types[point.type]++;
    });
    
    // Convert grid cells to clusters
    Object.entries(gridCells).forEach(([key, cell]) => {
        // Only include cells with multiple points
        if (cell.points.length >= 3) {
            // Determine cluster type based on dominant activity
            let maxType = '', maxCount = 0;
            Object.entries(cell.types).forEach(([type, count]) => {
                if (count > maxCount) {
                    maxCount = count;
                    maxType = type;
                }
            });
            
            clusters.push({
                center: cell.center,
                pointCount: cell.points.length,
                types: cell.types,
                dominant: maxType
            });
        }
    });
    
    // Sort clusters by point count
    clusters.sort((a, b) => b.pointCount - a.pointCount);
    
    return clusters;
}