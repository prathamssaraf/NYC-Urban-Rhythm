/**
 * Data loading and parsing functionality for NYC Urban Rhythm
 */

// State variables to store loaded data
let all311Data = [];
let transitData = [];
let taxiData = [];
let eventsData = [];
let dateRangeInitialized = false;

// Reference to DOM elements
const dom = {
    // File input elements
    fileInputs: {
        calls311: document.getElementById('csv311'),
        transit: document.getElementById('csvMTA'),
        taxi: document.getElementById('csvTLC'),
        events: document.getElementById('csvEvents')
    },
    // Date range controls
    dateControls: {
        start: document.getElementById('startDate'),
        end: document.getElementById('endDate'),
        updateBtn: document.getElementById('updateDashboardBtn')
    },
    // Info display
    infoPanel: document.getElementById('info-text')
};

/**
 * Initialize data loaders for all file inputs
 */
function initializeDataLoaders() {
    // Set up event listeners for file inputs
    dom.fileInputs.calls311.addEventListener('change', handle311DataLoad);
    dom.fileInputs.transit.addEventListener('change', handleTransitDataLoad);
    dom.fileInputs.taxi.addEventListener('change', handleTaxiDataLoad);
    dom.fileInputs.events.addEventListener('change', handleEventsDataLoad);
    
    // Disable date controls initially
    disableDateControls();
}

/**
 * Disable date range controls
 */
function disableDateControls() {
    dom.dateControls.start.disabled = true;
    dom.dateControls.end.disabled = true;
    dom.dateControls.updateBtn.disabled = true;
}

/**
 * Enable date range controls
 */
function enableDateControls() {
    dom.dateControls.start.disabled = false;
    dom.dateControls.end.disabled = false;
    dom.dateControls.updateBtn.disabled = false;
}

/**
 * Handle loading of 311 call data 
 */
function handle311DataLoad() {
    disableDateControls();
    showLoading('Loading 311 data...');
    all311Data = [];
    
    try {
        const files = Array.from(dom.fileInputs.calls311.files);
        if (!files.length) {
            hideLoading();
            return;
        }
        
        let completed = 0;
        
        files.forEach(file => {
            Papa.parse(file, {
                header: true,
                skipEmptyLines: true,
                worker: true,
                step: row => {
                    const d = parseDateFlexible(row.data.created_date);
                    if (!d) return;
                    
                    const pd = formatDate(d);
                    const time = d.toTimeString().substring(0, 5);
                    const hour = d.getHours();
                    const dow = d.getDay();
                    const rawBoro = (row.data.borough || '').trim();
                    
                    // Format borough name consistently
                    const borough = rawBoro
                        .toLowerCase()
                        .split(' ')
                        .map(w => w[0]?.toUpperCase() + w.slice(1))
                        .join(' ');
                        
                    const complaint = (row.data.complaint_type || '').trim();
                    const descriptor = (row.data.descriptor || '').trim();
                    
                    // Only include records with valid boroughs
                    if (Object.keys(BOROUGH_CENTROIDS).includes(borough)) {
                        all311Data.push({ 
                            parsed_date: pd, 
                            time, 
                            hour, 
                            dow,
                            borough, 
                            complaint,
                            descriptor,
                            lat: parseFloat(row.data.latitude || 0),
                            lng: parseFloat(row.data.longitude || 0),
                            dataset: 'calls311'
                        });
                    }
                },
                complete: () => { 
                    completed++;
                    if (completed === files.length) {
                        finalize311Data();
                    }
                },
                error: err => {
                    console.error('Error parsing 311 CSV:', err);
                    completed++;
                    if (completed === files.length) {
                        finalize311Data();
                    }
                }
            });
        });
        
        updateInfoText(`Processing ${files.length} 311 data files...`);
    } catch (error) {
        console.error('Error loading 311 data:', error);
        updateInfoText('Error loading 311 data.');
        hideLoading();
    }
}

/**
 * Finalize 311 data loading
 */
function finalize311Data() {
    hideLoading();
    
    if (!all311Data.length) {
        updateInfoText('No valid 311 data found.');
        return;
    }
    
    // Find min/max dates
    let minTime = Infinity, maxTime = -Infinity;
    
    all311Data.forEach(item => {
        const time = new Date(item.parsed_date).getTime();
        if (time < minTime) minTime = time;
        if (time > maxTime) maxTime = time;
    });
    
    const minDate = formatDate(new Date(minTime));
    const maxDate = formatDate(new Date(maxTime));
    
    // Set date controls
    dom.dateControls.start.min = dom.dateControls.end.min = minDate;
    dom.dateControls.start.max = dom.dateControls.end.max = maxDate;
    dom.dateControls.start.value = minDate;
    dom.dateControls.end.value = maxDate;
    
    // Enable controls
    enableDateControls();
    
    // Update info
    updateInfoText(`Loaded ${all311Data.length} 311 records. Range: ${minDate} to ${maxDate}`);
    
    // Mark date range as initialized and show timeline
    dateRangeInitialized = true;
    document.getElementById('timeline-control').classList.remove('hidden');
}

/**
 * Handle loading of MTA transit data
 */
function handleTransitDataLoad() {
    disableDateControls();
    showLoading('Loading transit data...');
    transitData = [];
    
    try {
        const files = Array.from(dom.fileInputs.transit.files);
        if (!files.length) {
            hideLoading();
            return;
        }
        
        let completed = 0;
        
        files.forEach(file => {
            Papa.parse(file, {
                header: true,
                skipEmptyLines: true,
                worker: true,
                step: row => {
                    const timestamp = parseDateFlexible(row.data.transit_timestamp);
                    if (!timestamp) return;
                    
                    const pd = formatDate(timestamp);
                    const time = timestamp.toTimeString().substring(0, 5);
                    const hour = timestamp.getHours();
                    const dow = timestamp.getDay();
                    
                    // Parse georeference if available
                    let lat = null, lng = null;
                    if (row.data.latitude && row.data.longitude) {
                        lat = parseFloat(row.data.latitude);
                        lng = parseFloat(row.data.longitude);
                    } else if (row.data.georeference) {
                        try {
                            const geoData = JSON.parse(row.data.georeference.replace(/'/g, '"'));
                            if (geoData.coordinates && geoData.coordinates.length >= 2) {
                                lng = geoData.coordinates[0];
                                lat = geoData.coordinates[1];
                            }
                        } catch (e) {
                            // Unable to parse georeference
                        }
                    }
                    
                    const ridership = parseFloat(row.data.ridership) || 0;
                    const borough = row.data.borough;
                    
                    if (borough && Object.keys(BOROUGH_CENTROIDS).includes(borough)) {
                        transitData.push({
                            transit_timestamp: row.data.transit_timestamp,
                            parsed_date: pd,
                            time,
                            hour,
                            dow,
                            station_complex: row.data.station_complex,
                            station_complex_id: row.data.station_complex_id,
                            borough,
                            ridership,
                            lat,
                            lng,
                            payment_method: row.data.payment_method,
                            dataset: 'transit'
                        });
                    }
                },
                complete: () => {
                    completed++;
                    if (completed === files.length) {
                        finalizeTransitData();
                    }
                },
                error: err => {
                    console.error('Error parsing transit CSV:', err);
                    completed++;
                    if (completed === files.length) {
                        finalizeTransitData();
                    }
                }
            });
        });
        
        updateInfoText(`Processing ${files.length} transit data files...`);
    } catch (error) {
        console.error('Error loading transit data:', error);
        updateInfoText('Error loading transit data.');
        hideLoading();
    }
}

/**
 * Finalize transit data loading
 */
function finalizeTransitData() {
    hideLoading();
    
    if (!transitData.length) {
        updateInfoText('No valid transit data found.');
        return;
    }
    
    // Find min/max dates
    let minTime = Infinity, maxTime = -Infinity;
    
    transitData.forEach(item => {
        const time = new Date(item.parsed_date).getTime();
        if (time < minTime) minTime = time;
        if (time > maxTime) maxTime = time;
    });
    
    const minDate = formatDate(new Date(minTime));
    const maxDate = formatDate(new Date(maxTime));
    
    // Set date controls if not already initialized
    if (!dateRangeInitialized) {
        dom.dateControls.start.min = dom.dateControls.end.min = minDate;
        dom.dateControls.start.max = dom.dateControls.end.max = maxDate;
        dom.dateControls.start.value = minDate;
        dom.dateControls.end.value = maxDate;
        
        // Enable controls
        enableDateControls();
        
        // Mark date range as initialized and show timeline
        dateRangeInitialized = true;
        document.getElementById('timeline-control').classList.remove('hidden');
    }
    
    // Update info
    updateInfoText(`Loaded ${transitData.length} transit records. Range: ${minDate} to ${maxDate}`);
}

/**
 * Handle loading of TLC taxi trip data
 */
function handleTaxiDataLoad() {
    disableDateControls();
    showLoading('Loading taxi data...');
    taxiData = [];
    
    try {
        const files = Array.from(dom.fileInputs.taxi.files);
        if (!files.length) {
            hideLoading();
            return;
        }
        
        let completed = 0;
        
        files.forEach(file => {
            Papa.parse(file, {
                header: true,
                skipEmptyLines: true,
                worker: true,
                step: row => {
                    // Try to get the pickup and dropoff timestamps
                    const pickupTime = row.data.pickup_datetime || row.data.tpep_pickup_datetime;
                    const dropoffTime = row.data.dropoff_datetime || row.data.tpep_dropoff_datetime;
                    
                    if (!pickupTime) return;
                    
                    const pickup = parseDateFlexible(pickupTime);
                    const dropoff = parseDateFlexible(dropoffTime);
                    
                    if (!pickup || !dropoff) return;
                    
                    const pd = formatDate(pickup);
                    const time = pickup.toTimeString().substring(0, 5);
                    const hour = pickup.getHours();
                    const dow = pickup.getDay();
                    
                    // Get zone IDs
                    const puLocationId = parseInt(row.data.pulocationid);
                    const doLocationId = parseInt(row.data.dolocationid);
                    
                    // Try to determine borough from location IDs
                    const puBorough = ZONE_TO_BOROUGH[puLocationId];
                    const doBorough = ZONE_TO_BOROUGH[doLocationId];
                    
                    // Only include if we can determine at least one borough
                    if (puBorough || doBorough) {
                        taxiData.push({
                            pickup_datetime: pickupTime,
                            dropoff_datetime: dropoffTime,
                            parsed_date: pd,
                            time,
                            hour,
                            dow,
                            trip_distance: parseFloat(row.data.trip_distance) || 0,
                            passenger_count: parseInt(row.data.passenger_count) || 1,
                            fare_amount: parseFloat(row.data.fare_amount) || 0,
                            tip_amount: parseFloat(row.data.tip_amount) || 0,
                            total_amount: parseFloat(row.data.total_amount) || 0,
                            pu_location_id: puLocationId,
                            do_location_id: doLocationId,
                            pu_borough: puBorough,
                            do_borough: doBorough,
                            dataset: 'taxi'
                        });
                    }
                },
                complete: () => {
                    completed++;
                    if (completed === files.length) {
                        finalizeTaxiData();
                    }
                },
                error: err => {
                    console.error('Error parsing taxi CSV:', err);
                    completed++;
                    if (completed === files.length) {
                        finalizeTaxiData();
                    }
                }
            });
        });
        
        updateInfoText(`Processing ${files.length} taxi data files...`);
    } catch (error) {
        console.error('Error loading taxi data:', error);
        updateInfoText('Error loading taxi data.');
        hideLoading();
    }
}

/**
 * Finalize taxi data loading
 */
function finalizeTaxiData() {
    hideLoading();
    
    if (!taxiData.length) {
        updateInfoText('No valid taxi data found.');
        return;
    }
    
    // Find min/max dates
    let minTime = Infinity, maxTime = -Infinity;
    
    taxiData.forEach(item => {
        const time = new Date(item.parsed_date).getTime();
        if (time < minTime) minTime = time;
        if (time > maxTime) maxTime = time;
    });
    
    const minDate = formatDate(new Date(minTime));
    const maxDate = formatDate(new Date(maxTime));
    
    // Set date controls if not already initialized
    if (!dateRangeInitialized) {
        dom.dateControls.start.min = dom.dateControls.end.min = minDate;
        dom.dateControls.start.max = dom.dateControls.end.max = maxDate;
        dom.dateControls.start.value = minDate;
        dom.dateControls.end.value = maxDate;
        
        // Enable controls
        enableDateControls();
        
        // Mark date range as initialized and show timeline
        dateRangeInitialized = true;
        document.getElementById('timeline-control').classList.remove('hidden');
    }
    
    // Update info
    updateInfoText(`Loaded ${taxiData.length} taxi records. Range: ${minDate} to ${maxDate}`);
}

/**
 * Handle loading of NYC events data
 */
function handleEventsDataLoad() {
    disableDateControls();
    showLoading('Loading events data...');
    eventsData = [];
    
    try {
        const files = Array.from(dom.fileInputs.events.files);
        if (!files.length) {
            hideLoading();
            return;
        }
        
        let completed = 0;
        
        files.forEach(file => {
            Papa.parse(file, {
                header: true,
                skipEmptyLines: true,
                worker: true,
                step: row => {
                    const startTime = row.data.start_datetime || row.data.start_date_time;
                    const endTime = row.data.end_datetime || row.data.end_date_time;
                    
                    if (!startTime) return;
                    
                    const start = parseDateFlexible(startTime);
                    const end = parseDateFlexible(endTime);
                    
                    if (!start) return;
                    
                    const pd = formatDate(start);
                    const time = start.toTimeString().substring(0, 5);
                    const hour = start.getHours();
                    const dow = start.getDay();
                    
                    const borough = row.data.event_borough;
                    const location = row.data.event_location;
                    
                    if (borough && Object.keys(BOROUGH_CENTROIDS).includes(borough)) {
                        // Use borough centroid as default location
                        const [lat, lng] = BOROUGH_CENTROIDS[borough];
                        
                        eventsData.push({
                            event_id: row.data.event_id,
                            event_name: row.data.event_name,
                            start_datetime: startTime,
                            end_datetime: endTime,
                            parsed_date: pd,
                            time,
                            hour,
                            dow,
                            event_agency: row.data.event_agency,
                            event_type: row.data.event_type,
                            borough,
                            location,
                            lat,
                            lng,
                            dataset: 'events'
                        });
                    }
                },
                complete: () => {
                    completed++;
                    if (completed === files.length) {
                        // First geocode events, then finalize
                        geocodeEventLocations().finally(() => {
                            finalizeEventsData();
                        });
                    }
                },
                error: err => {
                    console.error('Error parsing events CSV:', err);
                    completed++;
                    if (completed === files.length) {
                        // Even if parsing had errors, try to geocode and finalize
                        geocodeEventLocations().finally(() => {
                            finalizeEventsData();
                        });
                    }
                }
            });
        });
        
        updateInfoText(`Processing ${files.length} event data files...`);
    } catch (error) {
        console.error('Error loading events data:', error);
        updateInfoText('Error loading events data.');
        hideLoading();
    }
}

/**
 * Attempt to geocode event locations to get better coordinates
 */
async function geocodeEventLocations() {
    // Only process a few events to avoid rate limits
    const MAX_GEOCODING = 20;
    const eventsToGeocode = eventsData
        .filter(event => event.location && event.location.length > 3)
        .slice(0, MAX_GEOCODING);
    
    for (const event of eventsToGeocode) {
        try {
            const query = `${event.location}, ${event.borough}, New York City`;
            const url = `${API_CONFIG.geocodingEndpoint}${encodeURIComponent(query)}.json?access_token=${MAPBOX_ACCESS_TOKEN}&limit=1`;
            
            const response = await fetch(url);
            const data = await response.json();
            
            if (data.features && data.features.length > 0) {
                const [lng, lat] = data.features[0].center;
                event.lng = lng;
                event.lat = lat;
            }
            
            // Add a small delay to avoid rate limits
            await new Promise(resolve => setTimeout(resolve, 300));
        } catch (error) {
            console.error('Error geocoding event location:', error);
        }
    }
}

/**
 * Finalize events data loading
 */
function finalizeEventsData() {
    hideLoading();
    
    if (!eventsData.length) {
        updateInfoText('No valid events data found.');
        return;
    }
    
    // Find min/max dates
    let minTime = Infinity, maxTime = -Infinity;
    
    eventsData.forEach(item => {
        const time = new Date(item.parsed_date).getTime();
        if (time < minTime) minTime = time;
        if (time > maxTime) maxTime = time;
    });
    
    const minDate = formatDate(new Date(minTime));
    const maxDate = formatDate(new Date(maxTime));
    
    // Set date controls if not already initialized
    if (!dateRangeInitialized) {
        dom.dateControls.start.min = dom.dateControls.end.min = minDate;
        dom.dateControls.start.max = dom.dateControls.end.max = maxDate;
        dom.dateControls.start.value = minDate;
        dom.dateControls.end.value = maxDate;
        
        // Enable controls
        enableDateControls();
        
        // Mark date range as initialized and show timeline
        dateRangeInitialized = true;
        document.getElementById('timeline-control').classList.remove('hidden');
    }
    
    // Update info
    updateInfoText(`Loaded ${eventsData.length} event records. Range: ${minDate} to ${maxDate}`);
}

/**
 * Get all datasets filtered by date range
 */
function getFilteredDatasets(startDate, endDate) {
    // Filter all datasets by date range
    const filtered311 = all311Data.filter(item => 
        item.parsed_date >= startDate && item.parsed_date <= endDate
    );
    
    const filteredTransit = transitData.filter(item => 
        item.parsed_date >= startDate && item.parsed_date <= endDate
    );
    
    const filteredTaxi = taxiData.filter(item => 
        item.parsed_date >= startDate && item.parsed_date <= endDate
    );
    
    // For events, include those that overlap with the date range
    const filteredEvents = eventsData.filter(item => {
        const eventStart = new Date(item.start_datetime);
        const eventEnd = new Date(item.end_datetime);
        const rangeStart = new Date(startDate);
        const rangeEnd = new Date(endDate);
        
        // Check if event overlaps with date range
        return (eventStart <= rangeEnd && eventEnd >= rangeStart);
    });
    
    return {
        calls311: filtered311,
        transit: filteredTransit,
        taxi: filteredTaxi,
        events: filteredEvents
    };
}

/**
 * Get dataset statistics
 */
function getDatasetStats(filteredData) {
    const stats = {
        calls311: filteredData.calls311.length,
        transit: filteredData.transit.reduce((sum, item) => sum + (parseFloat(item.ridership) || 0), 0),
        taxi: filteredData.taxi.length,
        events: filteredData.events.length
    };
    
    return stats;
}

/**
 * Update stats summary in UI
 */
function updateStatsSummary(stats) {
    document.getElementById('stat-311').textContent = `311 Calls: ${formatNumber(stats.calls311)}`;
    document.getElementById('stat-transit').textContent = `Subway Entries: ${formatNumber(Math.round(stats.transit))}`;
    document.getElementById('stat-taxi').textContent = `Taxi Trips: ${formatNumber(stats.taxi)}`;
    document.getElementById('stat-events').textContent = `Events: ${formatNumber(stats.events)}`;
    document.getElementById('stats-summary').classList.remove('hidden');
}