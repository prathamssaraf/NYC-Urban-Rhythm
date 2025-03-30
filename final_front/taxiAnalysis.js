/**
 * Taxi trip analysis functionality for NYC Urban Rhythm
 */

/**
 * Analyze taxi trip patterns
 * @param {Array} taxiData - Taxi trip data
 * @param {string} startDate - Start date in YYYY-MM-DD format
 * @param {string} endDate - End date in YYYY-MM-DD format
 * @returns {Object} - Taxi analysis results
 */
function analyzeTaxiPatterns(taxiData, startDate, endDate) {
    // Filter data by date range
    const filteredData = taxiData.filter(item => 
        item.parsed_date >= startDate && item.parsed_date <= endDate
    );
    
    // Initialize analysis object
    const analysis = {
        totalTrips: filteredData.length,
        byBorough: {
            pickup: {},
            dropoff: {}
        },
        byHour: Array(24).fill(0),
        byDayOfWeek: Array(7).fill(0),
        metrics: {
            avgDistance: 0,
            avgFare: 0,
            avgTip: 0,
            distanceByBorough: {},
            fareByBorough: {}
        },
        originDestination: {}
    };
    
    // Get valid boroughs
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Initialize borough counts
    validBoroughs.forEach(borough => {
        analysis.byBorough.pickup[borough] = 0;
        analysis.byBorough.dropoff[borough] = 0;
        analysis.metrics.distanceByBorough[borough] = [];
        analysis.metrics.fareByBorough[borough] = [];
        
        // Initialize origin-destination matrix
        analysis.originDestination[borough] = {};
        validBoroughs.forEach(dest => {
            analysis.originDestination[borough][dest] = 0;
        });
    });
    
    // Analyze data
    filteredData.forEach(record => {
        // Add to hourly count
        analysis.byHour[record.hour]++;
        
        // Add to day of week count
        analysis.byDayOfWeek[record.dow]++;
        
        // Add to pickup borough count
        if (record.pu_borough && analysis.byBorough.pickup[record.pu_borough] !== undefined) {
            analysis.byBorough.pickup[record.pu_borough]++;
            
            // Add to metrics by borough
            analysis.metrics.distanceByBorough[record.pu_borough].push(record.trip_distance);
            analysis.metrics.fareByBorough[record.pu_borough].push(record.fare_amount);
        }
        
        // Add to dropoff borough count
        if (record.do_borough && analysis.byBorough.dropoff[record.do_borough] !== undefined) {
            analysis.byBorough.dropoff[record.do_borough]++;
        }
        
        // Add to origin-destination matrix
        if (record.pu_borough && record.do_borough && 
            analysis.originDestination[record.pu_borough] && 
            analysis.originDestination[record.pu_borough][record.do_borough] !== undefined) {
            analysis.originDestination[record.pu_borough][record.do_borough]++;
        }
        
        // Add to overall metrics
        analysis.metrics.avgDistance += record.trip_distance;
        analysis.metrics.avgFare += record.fare_amount;
        analysis.metrics.avgTip += record.tip_amount;
    });
    
    // Calculate averages
    if (filteredData.length > 0) {
        analysis.metrics.avgDistance /= filteredData.length;
        analysis.metrics.avgFare /= filteredData.length;
        analysis.metrics.avgTip /= filteredData.length;
    }
    
    // Calculate borough averages
    validBoroughs.forEach(borough => {
        const distances = analysis.metrics.distanceByBorough[borough];
        const fares = analysis.metrics.fareByBorough[borough];
        
        analysis.metrics.distanceByBorough[borough] = distances.length > 0 
            ? distances.reduce((sum, val) => sum + val, 0) / distances.length 
            : 0;
            
        analysis.metrics.fareByBorough[borough] = fares.length > 0 
            ? fares.reduce((sum, val) => sum + val, 0) / fares.length 
            : 0;
    });
    
    return analysis;
}

/**
 * Find peak taxi activity periods
 * @param {Object} analysis - Taxi analysis results
 * @returns {Object} - Peak activity periods
 */
function findPeakTaxiPeriods(analysis) {
    // Find peak hour
    let peakHour = 0;
    let peakHourValue = 0;
    
    for (let i = 0; i < 24; i++) {
        if (analysis.byHour[i] > peakHourValue) {
            peakHourValue = analysis.byHour[i];
            peakHour = i;
        }
    }
    
    // Find peak day of week
    let peakDay = 0;
    let peakDayValue = 0;
    
    for (let i = 0; i < 7; i++) {
        if (analysis.byDayOfWeek[i] > peakDayValue) {
            peakDayValue = analysis.byDayOfWeek[i];
            peakDay = i;
        }
    }
    
    // Get peak origin-destination pair
    let peakOD = { origin: null, destination: null, trips: 0 };
    
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    validBoroughs.forEach(origin => {
        validBoroughs.forEach(destination => {
            const trips = analysis.originDestination[origin][destination];
            if (trips > peakOD.trips) {
                peakOD = { origin, destination, trips };
            }
        });
    });
    
    return {
        peakHour,
        peakDay,
        peakOD,
        peakHourValue,
        peakDayValue
    };
}

/**
 * Analyze correlation between taxi trips and 311 calls
 * @param {Object} taxiAnalysis - Taxi analysis results
 * @returns {Object} - Correlation analysis
 */
function analyzeTaxi311Correlation(taxiAnalysis) {
    // Initialize correlation object
    const correlation = {
        overall: null,
        byBorough: {},
        byHour: null,
        byDayOfWeek: null
    };
    
    // Get valid boroughs
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Calculate borough-level correlation
    validBoroughs.forEach(borough => {
        // Get daily taxi pickups and 311 calls for this borough
        const taxiSeries = [];
        const callsSeries = [];
        
        // Check if we have daily data for taxi and 311
        if (processedData.taxi.dailyData && processedData.calls311.dailyData) {
            // Get days that exist in both datasets
            const commonDays = Object.keys(processedData.taxi.dailyData).filter(date => 
                processedData.calls311.dailyData[date]
            );
            
            // Build data series
            commonDays.forEach(date => {
                taxiSeries.push(processedData.taxi.dailyData[date].byBorough[borough] || 0);
                callsSeries.push(processedData.calls311.dailyData[date].byBorough[borough] || 0);
            });
            
            // Calculate correlation if we have enough data points
            if (taxiSeries.length >= 3) {
                correlation.byBorough[borough] = calculateCorrelation(taxiSeries, callsSeries);
            }
        }
    });
    
    // Calculate hourly correlation
    if (processedData.taxi.hourlyByBorough && processedData.calls311.hourlyByBorough) {
        // Aggregate hourly data across all boroughs
        const taxiHourly = Array(24).fill(0);
        const callsHourly = Array(24).fill(0);
        
        validBoroughs.forEach(borough => {
            for (let hour = 0; hour < 24; hour++) {
                taxiHourly[hour] += processedData.taxi.hourlyByBorough[borough][hour] || 0;
                callsHourly[hour] += processedData.calls311.hourlyByBorough[borough][hour] || 0;
            }
        });
        
        // Calculate correlation
        correlation.byHour = calculateCorrelation(taxiHourly, callsHourly);
    }
    
    // Calculate day of week correlation
    if (processedData.taxi.dowByBorough && processedData.calls311.dowByBorough) {
        // Aggregate day of week data across all boroughs
        const taxiDOW = Array(7).fill(0);
        const callsDOW = Array(7).fill(0);
        
        validBoroughs.forEach(borough => {
            for (let dow = 0; dow < 7; dow++) {
                taxiDOW[dow] += processedData.taxi.dowByBorough[borough][dow] || 0;
                callsDOW[dow] += processedData.calls311.dowByBorough[borough][dow] || 0;
            }
        });
        
        // Calculate correlation
        correlation.byDayOfWeek = calculateCorrelation(taxiDOW, callsDOW);
    }
    
    return correlation;
}

/**
 * Generate taxi heatmap data
 * @param {Object} analysis - Taxi analysis results
 * @returns {Array} - GeoJSON features for heatmap
 */
function generateTaxiHeatmap(analysis) {
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    const features = [];
    
    // Create points based on borough activity
    validBoroughs.forEach(borough => {
        const pickups = analysis.byBorough.pickup[borough] || 0;
        
        // Skip if no pickups
        if (pickups === 0) return;
        
        // Get borough center
        const [lat, lng] = BOROUGH_CENTROIDS[borough];
        
        // Create proportional number of points
        const numPoints = Math.min(Math.ceil(pickups / 10), 100);
        
        for (let i = 0; i < numPoints; i++) {
            // Add random offset to create a spread
            const latOffset = (Math.random() - 0.5) * 0.05;
            const lngOffset = (Math.random() - 0.5) * 0.05;
            
            features.push({
                type: 'Feature',
                properties: {
                    intensity: pickups / numPoints
                },
                geometry: {
                    type: 'Point',
                    coordinates: [lng + lngOffset, lat + latOffset]
                }
            });
        }
    });
    
    return features;
}

/**
 * Generate origin-destination flow visualization
 * @param {Object} analysis - Taxi analysis results
 * @returns {Array} - Flow lines data
 */
function generateODFlowVisualization(analysis) {
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    const flows = [];
    
    // Create flow lines between boroughs
    validBoroughs.forEach(origin => {
        validBoroughs.forEach(destination => {
            // Skip self-loops
            if (origin === destination) return;
            
            const trips = analysis.originDestination[origin][destination];
            
            // Skip if no trips
            if (trips === 0) return;
            
            // Get borough centers
            const [originLat, originLng] = BOROUGH_CENTROIDS[origin];
            const [destLat, destLng] = BOROUGH_CENTROIDS[destination];
            
            // Calculate width based on trip count
            const maxTrips = Math.max(
                ...validBoroughs.flatMap(o => 
                    validBoroughs.map(d => 
                        analysis.originDestination[o][d]
                    )
                )
            );
            
            const width = Math.max(1, (trips / maxTrips) * 5);
            
            flows.push({
                origin: [originLng, originLat],
                destination: [destLng, destLat],
                trips,
                width,
                color: BOROUGH_COLORS[origin]
            });
        });
    });
    
    // Sort by trip count (to draw smaller flows on top)
    flows.sort((a, b) => b.trips - a.trips);
    
    return flows;
}

/**
 * Update taxi visualization on the map
 * @param {Object} analysis - Taxi analysis results
 */
function updateTaxiMapVisualization(analysis) {
    // Remove existing taxi layer if it exists
    if (map.getLayer('taxi-heatmap')) {
        map.removeLayer('taxi-heatmap');
    }
    
    if (map.getSource('taxi-points')) {
        map.removeSource('taxi-points');
    }
    
    // Generate heatmap data
    const heatmapData = generateTaxiHeatmap(analysis);
    
    // Add source if we have points
    if (heatmapData.length > 0) {
        // Add source
        map.addSource('taxi-points', {
            type: 'geojson',
            data: {
                type: 'FeatureCollection',
                features: heatmapData
            }
        });
        
        // Add heatmap layer
        map.addLayer({
            id: 'taxi-heatmap',
            type: 'heatmap',
            source: 'taxi-points',
            paint: {
                'heatmap-weight': ['get', 'intensity'],
                'heatmap-intensity': 0.6,
                'heatmap-color': [
                    'interpolate',
                    ['linear'],
                    ['heatmap-density'],
                    0, 'rgba(236,222,239,0)',
                    0.2, 'rgb(250,219,134)',
                    0.4, 'rgb(245,186,82)',
                    0.6, 'rgb(238,150,33)',
                    0.8, 'rgb(232,114,14)',
                    1, 'rgb(204,80,16)'
                ],
                'heatmap-radius': 15,
                'heatmap-opacity': 0.7
            },
            layout: {
                visibility: document.getElementById('layer-taxi').checked ? 'visible' : 'none'
            }
        });
    }
    
    // Add origin-destination flow lines for top flows
    const flows = generateODFlowVisualization(analysis);
    
    // Top 5 flows only
    flows.slice(0, 5).forEach(flow => {
        // Create a curved line between points
        const midpoint = [
            (flow.origin[0] + flow.destination[0]) / 2,
            (flow.origin[1] + flow.destination[1]) / 2 - 0.05
        ];
        
        // Create a GeoJSON line feature with curved path
        const lineData = {
            type: 'Feature',
            properties: {
                trips: flow.trips,
                color: flow.color,
                width: flow.width
            },
            geometry: {
                type: 'LineString',
                coordinates: [
                    flow.origin,
                    midpoint,
                    flow.destination
                ]
            }
        };
        
        // Add source if it doesn't exist
        const sourceId = `od-flow-${flow.origin[0]}-${flow.origin[1]}-${flow.destination[0]}-${flow.destination[1]}`;
        
        if (!map.getSource(sourceId)) {
            map.addSource(sourceId, {
                type: 'geojson',
                data: lineData
            });
            
            // Add line layer
            map.addLayer({
                id: sourceId,
                type: 'line',
                source: sourceId,
                layout: {
                    'line-join': 'round',
                    'line-cap': 'round',
                    visibility: document.getElementById('layer-taxi').checked ? 'visible' : 'none'
                },
                paint: {
                    'line-color': flow.color,
                    'line-width': flow.width,
                    'line-opacity': 0.6
                }
            });
        }
    });
}