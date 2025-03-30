/**
 * Transit analysis functionality for NYC Urban Rhythm
 */

/**
 * Analyze transit ridership patterns
 * @param {Array} transitData - Transit ridership data
 * @param {string} startDate - Start date in YYYY-MM-DD format
 * @param {string} endDate - End date in YYYY-MM-DD format
 * @returns {Object} - Transit analysis results
 */
function analyzeTransitPatterns(transitData, startDate, endDate) {
    // Filter data by date range
    const filteredData = transitData.filter(item => 
        item.parsed_date >= startDate && item.parsed_date <= endDate
    );
    
    // Initialize analysis object
    const analysis = {
        totalRidership: 0,
        byBorough: {},
        byStation: {},
        byHour: Array(24).fill(0),
        byDayOfWeek: Array(7).fill(0),
        byFareType: {}
    };
    
    // Get valid boroughs
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Initialize borough counts
    validBoroughs.forEach(borough => {
        analysis.byBorough[borough] = 0;
    });
    
    // Analyze data
    filteredData.forEach(record => {
        const ridership = parseFloat(record.ridership) || 0;
        
        // Add to total
        analysis.totalRidership += ridership;
        
        // Add to borough total
        if (analysis.byBorough[record.borough] !== undefined) {
            analysis.byBorough[record.borough] += ridership;
        }
        
        // Add to station total
        if (!analysis.byStation[record.station_complex]) {
            analysis.byStation[record.station_complex] = {
                id: record.station_complex_id,
                total: 0,
                byHour: Array(24).fill(0),
                lat: record.lat,
                lng: record.lng,
                borough: record.borough
            };
        }
        analysis.byStation[record.station_complex].total += ridership;
        
        // Add to hourly total
        analysis.byHour[record.hour] += ridership;
        
        // Add to day of week total
        analysis.byDayOfWeek[record.dow] += ridership;
        
        // Add to fare type total
        const fareType = record.payment_method || 'Unknown';
        if (!analysis.byFareType[fareType]) {
            analysis.byFareType[fareType] = 0;
        }
        analysis.byFareType[fareType] += ridership;
        
        // Also track station hourly patterns
        analysis.byStation[record.station_complex].byHour[record.hour] += ridership;
    });
    
    return analysis;
}

/**
 * Find transit "hotspots" - stations with unusually high ridership
 * @param {Object} analysis - Transit analysis results
 * @returns {Array} - Transit hotspots
 */
function findTransitHotspots(analysis) {
    // Get stations sorted by total ridership
    const stations = Object.entries(analysis.byStation)
        .map(([name, data]) => ({ name, ...data }))
        .filter(station => station.lat && station.lng) // Only include stations with coordinates
        .sort((a, b) => b.total - a.total);
    
    // Calculate average ridership
    const totalStations = stations.length;
    const totalRidership = stations.reduce((sum, station) => sum + station.total, 0);
    const avgRidership = totalStations > 0 ? totalRidership / totalStations : 0;
    
    // Find hotspots (stations with ridership > 2x average)
    const hotspots = stations.filter(station => station.total > avgRidership * 2);
    
    return hotspots;
}

/**
 * Analyze correlation between transit ridership and 311 calls
 * @param {Object} transitAnalysis - Transit analysis results
 * @returns {Object} - Correlation analysis
 */
function analyzeTransit311Correlation(transitAnalysis) {
    // Initialize correlation object
    const correlation = {
        overall: null,
        byBorough: {},
        byHour: null,
        byDayOfWeek: null,
        stationProximity: []
    };
    
    // Get valid boroughs
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Calculate borough-level correlation
    validBoroughs.forEach(borough => {
        // Get daily transit ridership and 311 calls for this borough
        const transitSeries = [];
        const callsSeries = [];
        
        // Check if we have daily data for transit and 311
        if (processedData.transit.dailyData && processedData.calls311.dailyData) {
            // Get days that exist in both datasets
            const commonDays = Object.keys(processedData.transit.dailyData).filter(date => 
                processedData.calls311.dailyData[date]
            );
            
            // Build data series
            commonDays.forEach(date => {
                transitSeries.push(processedData.transit.dailyData[date].byBorough[borough] || 0);
                callsSeries.push(processedData.calls311.dailyData[date].byBorough[borough] || 0);
            });
            
            // Calculate correlation if we have enough data points
            if (transitSeries.length >= 3) {
                correlation.byBorough[borough] = calculateCorrelation(transitSeries, callsSeries);
            }
        }
    });
    
    // Calculate hourly correlation
    if (processedData.transit.hourlyByBorough && processedData.calls311.hourlyByBorough) {
        // Aggregate hourly data across all boroughs
        const transitHourly = Array(24).fill(0);
        const callsHourly = Array(24).fill(0);
        
        validBoroughs.forEach(borough => {
            for (let hour = 0; hour < 24; hour++) {
                transitHourly[hour] += processedData.transit.hourlyByBorough[borough][hour] || 0;
                callsHourly[hour] += processedData.calls311.hourlyByBorough[borough][hour] || 0;
            }
        });
        
        // Calculate correlation
        correlation.byHour = calculateCorrelation(transitHourly, callsHourly);
    }
    
    // Calculate day of week correlation
    if (processedData.transit.dowByBorough && processedData.calls311.dowByBorough) {
        // Aggregate day of week data across all boroughs
        const transitDOW = Array(7).fill(0);
        const callsDOW = Array(7).fill(0);
        
        validBoroughs.forEach(borough => {
            for (let dow = 0; dow < 7; dow++) {
                transitDOW[dow] += processedData.transit.dowByBorough[borough][dow] || 0;
                callsDOW[dow] += processedData.calls311.dowByBorough[borough][dow] || 0;
            }
        });
        
        // Calculate correlation
        correlation.byDayOfWeek = calculateCorrelation(transitDOW, callsDOW);
    }
    
    // Analyze station proximity correlation
    // Check if we have station data and 311 data with coordinates
    if (transitAnalysis.byStation && all311Data && all311Data.length > 0) {
        // Get top stations
        const topStations = Object.entries(transitAnalysis.byStation)
            .map(([name, data]) => ({ name, ...data }))
            .filter(station => station.lat && station.lng)
            .sort((a, b) => b.total - a.total)
            .slice(0, 10);
        
        // For each station, count nearby 311 calls
        topStations.forEach(station => {
            // Count 311 calls within 500 meters
            const nearbyCalls = all311Data.filter(call => {
                if (!call.lat || !call.lng) return false;
                
                const distance = calculateDistance(
                    station.lat, station.lng,
                    call.lat, call.lng
                );
                
                return distance <= 500; // 500 meters radius
            }).length;
            
            correlation.stationProximity.push({
                station: station.name,
                ridership: station.total,
                nearbyCalls,
                ratio: station.total > 0 ? nearbyCalls / station.total : 0
            });
        });
        
        // Sort by ratio
        correlation.stationProximity.sort((a, b) => b.ratio - a.ratio);
    }
    
    return correlation;
}

/**
 * Generate transit heatmap data
 * @param {Object} analysis - Transit analysis results
 * @returns {Array} - GeoJSON features for heatmap
 */
function generateTransitHeatmap(analysis) {
    // Convert station data to GeoJSON points
    const features = [];
    
    Object.entries(analysis.byStation).forEach(([name, data]) => {
        if (!data.lat || !data.lng) return;
        
        features.push({
            type: 'Feature',
            properties: {
                name,
                ridership: data.total,
                borough: data.borough
            },
            geometry: {
                type: 'Point',
                coordinates: [data.lng, data.lat]
            }
        });
    });
    
    return features;
}

/**
 * Generate transit flow visualization
 * @param {Object} analysis - Transit analysis results
 * @returns {Object} - Flow visualization data
 */
function generateTransitFlowVisualization(analysis) {
    // Initialize hours data
    const hourlyData = Array(24).fill().map((_, i) => ({
        hour: i,
        total: 0,
        byBorough: {}
    }));
    
    // Get valid boroughs
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Initialize borough values
    validBoroughs.forEach(borough => {
        hourlyData.forEach(hour => {
            hour.byBorough[borough] = 0;
        });
    });
    
    // Aggregate hourly data by borough
    if (processedData.transit.hourlyByBorough) {
        validBoroughs.forEach(borough => {
            for (let hour = 0; hour < 24; hour++) {
                const ridership = processedData.transit.hourlyByBorough[borough][hour] || 0;
                hourlyData[hour].byBorough[borough] = ridership;
                hourlyData[hour].total += ridership;
            }
        });
    }
    
    // Find peak and low hours
    const peakHour = [...hourlyData].sort((a, b) => b.total - a.total)[0]?.hour || 8;
    const lowHour = [...hourlyData].sort((a, b) => a.total - b.total)[0]?.hour || 3;
    
    // Generate visualization data
    return {
        hourlyData,
        peakHour,
        lowHour,
        stations: Object.entries(analysis.byStation)
            .map(([name, data]) => ({ name, ...data }))
            .filter(station => station.lat && station.lng)
            .sort((a, b) => b.total - a.total)
            .slice(0, 30)
    };
}

/**
 * Update transit visualization on the map
 * @param {Object} analysis - Transit analysis results
 */
function updateTransitMapVisualization(analysis) {
    // Clear existing transit markers
    markers.transit.forEach(marker => marker.remove());
    markers.transit = [];
    
    // Add top station markers
    const stations = Object.entries(analysis.byStation)
        .map(([name, data]) => ({ name, ...data }))
        .filter(station => station.lat && station.lng)
        .sort((a, b) => b.total - a.total)
        .slice(0, 30);
    
    // Create marker for each station
    stations.forEach(station => {
        const el = createMarkerElement('transit');
        
        // Scale marker size based on ridership
        const maxRidership = stations[0].total;
        const scale = 0.5 + (station.total / maxRidership) * 1.5;
        el.style.width = `${12 * scale}px`;
        el.style.height = `${12 * scale}px`;
        
        // Create popup content
        const popupContent = `
            <div class="font-medium text-sm">${station.name}</div>
            <div class="text-xs">${station.borough}</div>
            <div class="text-xs">Entries: ${formatNumber(Math.round(station.total))}</div>
        `;
        
        // Create popup
        const popup = new mapboxgl.Popup({ offset: 10 })
            .setHTML(popupContent);
        
        // Create and add marker
        const marker = new mapboxgl.Marker({ element: el })
            .setLngLat([station.lng, station.lat])
            .setPopup(popup)
            .addTo(map);
        
        // Store reference
        markers.transit.push(marker);
        
        // Hide if transit layer is disabled
        if (!document.getElementById('layer-transit').checked) {
            el.style.display = 'none';
        }
    });
}