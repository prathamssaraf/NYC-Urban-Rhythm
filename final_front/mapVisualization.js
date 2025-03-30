/**
 * Map visualization functionality for NYC Urban Rhythm
 */

// Map instance
let map;

// Layer references
let markers = {
    transit: [],
    events: [],
    taxi: []
};

// Cluster markers
let clusterMarkers = [];

/**
 * Initialize the map
 */
function initializeMap() {
    // Initialize map with Mapbox
    mapboxgl.accessToken = MAPBOX_ACCESS_TOKEN;
    
    map = new mapboxgl.Map({
        container: 'map',
        style: 'mapbox://styles/mapbox/light-v10',
        center: [-73.985, 40.748],
        zoom: 9.5,
        maxBounds: [[-74.3, 40.45], [-73.6, 40.95]]
    });
    
    // Add navigation controls
    map.addControl(new mapboxgl.NavigationControl());
    
    // Set up event handlers
    map.on('load', async () => {
        showLoading('Loading borough boundaries...');
        
        try {
            // Load borough boundaries
            const response = await fetch(GEOJSON_URL);
            const geojson = await response.json();
            
            // Add source
            map.addSource('boroughs', { 
                type: 'geojson', 
                data: geojson 
            });
            
            // Add fill layer
            map.addLayer({
                id: 'borough-fill',
                type: 'fill',
                source: 'boroughs',
                paint: { 
                    'fill-color': '#ccc', 
                    'fill-opacity': 0.6, 
                    'fill-outline-color': '#fff' 
                }
            });
            
            // Add line layer
            map.addLayer({
                id: 'borough-line',
                type: 'line',
                source: 'boroughs',
                paint: { 
                    'line-color': '#444', 
                    'line-width': 1.5 
                }
            });
            
            // Add hover layer
            map.addLayer({
                id: 'borough-hover',
                type: 'fill',
                source: 'boroughs',
                paint: { 
                    'fill-color': '#444', 
                    'fill-opacity': 0.3 
                },
                filter: ['==', ['get', 'name'], '']
            });
            
            // Set up hover effects
            setupBoroughHoverEffects();
        } catch (error) {
            console.error('Error loading borough boundaries:', error);
            updateInfoText('Error loading borough boundaries.');
        } finally {
            hideLoading();
        }
    });
    
    // Set up layer toggle handlers
    setupLayerToggles();
}

/**
 * Set up borough hover effects
 */
function setupBoroughHoverEffects() {
    // Popup for hover info
    const popup = new mapboxgl.Popup({
        closeButton: false,
        closeOnClick: false,
        maxWidth: '240px'
    });
    
    // Track hovered feature ID
    let hoveredFeatureId = null;
    
    // Mouse move handler
    map.on('mousemove', 'borough-fill', e => {
        if (!e.features.length) return;
        
        const feature = e.features[0];
        
        // Only update if we're hovering a new feature
        if (hoveredFeatureId !== feature.id) {
            hoveredFeatureId = feature.id;
            
            // Update hover filter
            map.setFilter('borough-hover', ['==', ['get', 'name'], feature.properties.name]);
            
            // Update cursor
            map.getCanvas().style.cursor = 'pointer';
            
            // Create popup content based on processed data
            let html = `<div class="font-medium text-sm">${feature.properties.name}</div>`;
            
            // Add 311 call data if available
            if (processedData.calls311.totalByBorough) {
                const calls = processedData.calls311.totalByBorough[feature.properties.name] || 0;
                html += `<div class="text-xs"><span class="data-indicator indicator-311"></span>311 Calls: ${formatNumber(calls)}</div>`;
            }
            
            // Add transit data if available
            if (processedData.transit.ridershipByBorough) {
                const ridership = processedData.transit.ridershipByBorough[feature.properties.name] || 0;
                html += `<div class="text-xs"><span class="data-indicator indicator-transit"></span>Subway Entries: ${formatNumber(Math.round(ridership))}</div>`;
            }
            
            // Add taxi data if available
            if (processedData.taxi.pickupsByBorough) {
                const pickups = processedData.taxi.pickupsByBorough[feature.properties.name] || 0;
                html += `<div class="text-xs"><span class="data-indicator indicator-taxi"></span>Taxi Pickups: ${formatNumber(pickups)}</div>`;
            }
            
            // Add events data if available
            if (processedData.events.eventsByBorough) {
                const events = processedData.events.eventsByBorough[feature.properties.name] || 0;
                html += `<div class="text-xs"><span class="data-indicator indicator-event"></span>Events: ${formatNumber(events)}</div>`;
            }
            
            // Show popup
            popup.setLngLat(e.lngLat)
                .setHTML(html)
                .addTo(map);
        }
    });
    
    // Mouse leave handler
    map.on('mouseleave', 'borough-fill', () => {
        hoveredFeatureId = null;
        map.setFilter('borough-hover', ['==', ['get', 'name'], '']);
        map.getCanvas().style.cursor = '';
        popup.remove();
    });
    
    // Click handler
    map.on('click', 'borough-fill', e => {
        if (!e.features.length) return;
        
        const feature = e.features[0];
        const boroughName = feature.properties.name;
        
        // Fly to borough
        map.flyTo({
            center: BOROUGH_CENTROIDS[boroughName],
            zoom: 11,
            duration: 1000
        });
        
        // Create a pulse effect
        createPulseEffect(map, BOROUGH_CENTROIDS[boroughName], BOROUGH_COLORS[boroughName]);
    });
}

/**
 * Set up layer toggle handlers
 */
function setupLayerToggles() {
    // 311 calls layer toggle
    document.getElementById('layer-311').addEventListener('change', e => {
        map.setPaintProperty(
            'borough-fill', 
            'fill-opacity', 
            e.target.checked ? 0.6 : 0
        );
    });
    
    // Transit layer toggle
    document.getElementById('layer-transit').addEventListener('change', e => {
        markers.transit.forEach(marker => {
            marker.getElement().style.display = e.target.checked ? 'block' : 'none';
        });
    });
    
    // Taxi layer toggle
    document.getElementById('layer-taxi').addEventListener('change', e => {
        if (map.getLayer('taxi-heatmap')) {
            map.setLayoutProperty(
                'taxi-heatmap',
                'visibility',
                e.target.checked ? 'visible' : 'none'
            );
        }
    });
    
    // Events layer toggle
    document.getElementById('layer-events').addEventListener('change', e => {
        markers.events.forEach(marker => {
            marker.getElement().style.display = e.target.checked ? 'block' : 'none';
        });
    });
}

/**
 * Update map visualization based on processed data
 */
function updateMapVisualization() {
    // Clear existing markers
    clearAllMarkers();
    
    // Update 311 calls visualization
    update311CallsLayer();
    
    // Update transit stations
    updateTransitStations();
    
    // Update taxi heatmap
    updateTaxiHeatmap();
    
    // Update events markers
    updateEventsMarkers();
    
    // Update activity clusters
    updateActivityClusters();
    
    // Update legend
    updateMapLegend();
}

/**
 * Clear all markers from the map
 */
function clearAllMarkers() {
    // Remove transit markers
    markers.transit.forEach(marker => marker.remove());
    markers.transit = [];
    
    // Remove event markers
    markers.events.forEach(marker => marker.remove());
    markers.events = [];
    
    // Remove taxi layer if it exists
    if (map.getLayer('taxi-heatmap')) {
        map.removeLayer('taxi-heatmap');
    }
    
    if (map.getSource('taxi-points')) {
        map.removeSource('taxi-points');
    }
    
    // Remove cluster markers
    clusterMarkers.forEach(marker => marker.remove());
    clusterMarkers = [];
}

/**
 * Update 311 calls layer
 */
function update311CallsLayer() {
    if (!processedData.calls311.totalByBorough) return;
    
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    const maxCalls = Math.max(...validBoroughs.map(b => processedData.calls311.totalByBorough[b] || 0), 1);
    const colorScale = d3.scaleSequential(d3.interpolateBlues).domain([0, maxCalls]);
    
    // Create expression for borough fill color
    const expression = ['match', ['get', 'name']];
    
    validBoroughs.forEach(borough => {
        const calls = processedData.calls311.totalByBorough[borough] || 0;
        expression.push(borough, colorScale(calls));
    });
    
    // Default color
    expression.push('#ccc');
    
    // Update borough fill color
    map.setPaintProperty('borough-fill', 'fill-color', expression);
}

/**
 * Update transit station markers
 */
function updateTransitStations() {
    // Check if we have station data
    if (!processedData.transit.stationRidership) return;
    
    // Only show top stations to avoid cluttering the map
    const stations = Object.values(processedData.transit.stationRidership)
        .filter(station => station.lat && station.lng)
        .sort((a, b) => b.total - a.total)
        .slice(0, 30);
    
    // Create marker for each station
    stations.forEach(station => {
        const el = createMarkerElement('transit');
        
        // Create popup content
        const popupContent = `
            <div class="font-medium text-sm">${station.id}: ${station.borough}</div>
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

/**
 * Update taxi heatmap
 */
function updateTaxiHeatmap() {
    // Check if we have taxi data
    if (!processedData.taxi.pickupsByBorough) return;
    
    // Since we don't have exact coordinates for all taxi pickups,
    // we'll create a simulated heatmap based on borough activity
    const points = [];
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    validBoroughs.forEach(borough => {
        const pickups = processedData.taxi.pickupsByBorough[borough] || 0;
        
        // Skip if no pickups
        if (pickups === 0) return;
        
        // Get borough center
        const [lat, lng] = BOROUGH_CENTROIDS[borough];
        
        // Create a number of points proportional to pickup count
        // We'll distribute them randomly around the borough center
        const numPoints = Math.min(Math.ceil(pickups / 10), 100);
        
        for (let i = 0; i < numPoints; i++) {
            // Add random offset to create a spread
            const latOffset = (Math.random() - 0.5) * 0.05;
            const lngOffset = (Math.random() - 0.5) * 0.05;
            
            points.push({
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
    
    // Add source if we have points
    if (points.length > 0) {
        // Add source
        map.addSource('taxi-points', {
            type: 'geojson',
            data: {
                type: 'FeatureCollection',
                features: points
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
}

/**
 * Update events markers
 */
function updateEventsMarkers() {
    // Check if we have events data
    if (!processedData.events.eventsList) return;
    
    // Create marker for each event
    processedData.events.eventsList.forEach(event => {
        // Skip if no coordinates
        if (!event.lat || !event.lng) return;
        
        const el = createMarkerElement('event', 16);
        
        // Create popup content
        const startDate = new Date(event.start);
        const formattedDate = startDate.toLocaleDateString();
        const popupContent = `
            <div class="font-medium text-sm">${event.name}</div>
            <div class="text-xs">${formattedDate}</div>
            <div class="text-xs">${event.borough}</div>
            <div class="text-xs">${event.type || 'No type specified'}</div>
        `;
        
        // Create popup
        const popup = new mapboxgl.Popup({ offset: 10 })
            .setHTML(popupContent);
        
        // Create and add marker
        const marker = new mapboxgl.Marker({ element: el })
            .setLngLat([event.lng, event.lat])
            .setPopup(popup)
            .addTo(map);
        
        // Store reference
        markers.events.push(marker);
        
        // Hide if events layer is disabled
        if (!document.getElementById('layer-events').checked) {
            el.style.display = 'none';
        }
    });
}

/**
 * Update activity clusters
 */
function updateActivityClusters() {
    // Detect clusters
    const clusters = detectActivityClusters();
    
    // Add cluster markers
    clusters.slice(0, 10).forEach(cluster => {
        // Create element for cluster
        const el = document.createElement('div');
        el.className = 'flex items-center justify-center rounded-full border-2 border-white shadow-md';
        el.style.width = '30px';
        el.style.height = '30px';
        el.style.fontSize = '10px';
        el.style.fontWeight = 'bold';
        el.style.color = 'white';
        
        // Set color based on dominant type
        let color = '#666';
        if (cluster.dominant === 'calls311') color = DATASET_COLORS.calls311;
        else if (cluster.dominant === 'transit') color = DATASET_COLORS.transit;
        else if (cluster.dominant === 'taxi') color = DATASET_COLORS.taxi;
        else if (cluster.dominant === 'event') color = DATASET_COLORS.events;
        
        el.style.backgroundColor = color;
        el.textContent = cluster.pointCount;
        
        // Create popup content
        let typesText = '';
        Object.entries(cluster.types).forEach(([type, count]) => {
            const pct = Math.round((count / cluster.pointCount) * 100);
            typesText += `<div class="text-xs">${type}: ${count} (${pct}%)</div>`;
        });
        
        const popupContent = `
            <div class="font-medium text-sm">Activity Cluster</div>
            <div class="text-xs">Total Points: ${cluster.pointCount}</div>
            ${typesText}
        `;
        
        // Create popup
        const popup = new mapboxgl.Popup({ offset: 15 })
            .setHTML(popupContent);
        
        // Create and add marker
        const marker = new mapboxgl.Marker({ element: el })
            .setLngLat([cluster.center.lng, cluster.center.lat])
            .setPopup(popup)
            .addTo(map);
        
        // Store reference
        clusterMarkers.push(marker);
    });
}

/**
 * Update map legend
 */
function updateMapLegend() {
    const legend = document.getElementById('legend');
    legend.innerHTML = '<h4 class="font-semibold text-xs mb-1">Data Layers</h4>';
    legend.classList.remove('hidden');
    
    // 311 calls legend
    if (processedData.calls311.totalByBorough) {
        const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
        const maxCalls = Math.max(...validBoroughs.map(b => processedData.calls311.totalByBorough[b] || 0), 1);
        const colorScale = d3.scaleSequential(d3.interpolateBlues).domain([0, maxCalls]);
        
        const section = document.createElement('div');
        section.className = 'mt-2';
        section.innerHTML = '<div class="text-xs font-medium">311 Calls</div>';
        
        // Create color steps
        const steps = 4;
        const stepSize = maxCalls / steps;
        
        for (let i = 0; i < steps; i++) {
            const low = Math.round(i * stepSize);
            const high = Math.round((i + 1) * stepSize);
            const color = colorScale(low + stepSize / 2);
            
            const item = document.createElement('div');
            item.className = 'legend-item text-xs';
            item.innerHTML = `
                <span class="legend-color" style="background-color:${color}"></span>
                <span>${formatNumber(low)}–${formatNumber(high)}</span>
            `;
            
            section.appendChild(item);
        }
        
        legend.appendChild(section);
    }
    
    // Transit stations legend
    if (markers.transit.length > 0) {
        const section = document.createElement('div');
        section.className = 'mt-2';
        section.innerHTML = `
            <div class="text-xs font-medium">Transit Stations</div>
            <div class="legend-item text-xs">
                <div class="marker-transit" style="width:10px;height:10px;"></div>
                <span class="ml-1">Top stations by ridership</span>
            </div>
        `;
        
        legend.appendChild(section);
    }
    
    // Taxi heatmap legend
    if (map.getLayer('taxi-heatmap')) {
        const section = document.createElement('div');
        section.className = 'mt-2';
        section.innerHTML = `
            <div class="text-xs font-medium">Taxi Activity</div>
            <div class="legend-item text-xs">
                <div class="flex h-2">
                    <div style="width:30px;background:linear-gradient(to right,rgb(250,219,134),rgb(204,80,16))"></div>
                </div>
                <span class="ml-1">Pickup density</span>
            </div>
        `;
        
        legend.appendChild(section);
    }
    
    // Events legend
    if (markers.events.length > 0) {
        const section = document.createElement('div');
        section.className = 'mt-2';
        section.innerHTML = `
            <div class="text-xs font-medium">Events</div>
            <div class="legend-item text-xs">
                <div class="marker-event" style="width:12px;height:12px;"></div>
                <span class="ml-1">Event locations</span>
            </div>
        `;
        
        legend.appendChild(section);
    }
    
    // Clusters legend
    if (clusterMarkers.length > 0) {
        const section = document.createElement('div');
        section.className = 'mt-2';
        section.innerHTML = `
            <div class="text-xs font-medium">Activity Clusters</div>
            <div class="legend-item text-xs">
                <div style="width:12px;height:12px;border-radius:50%;background-color:#666;color:white;font-size:8px;text-align:center;line-height:12px;">N</div>
                <span class="ml-1">N points of activity</span>
            </div>
        `;
        
        legend.appendChild(section);
    }
}