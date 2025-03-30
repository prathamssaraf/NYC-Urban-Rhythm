// Configuration settings for NYC Urban Rhythm

// Mapbox access token
const MAPBOX_ACCESS_TOKEN = 'pk.eyJ1IjoicHJhdGhhbXNhcmFmMDA3IiwiYSI6ImNtOXljeWExajFlOTYyanBzdmh2YXplNXMifQ.UCm1kg1UeqCrz0j16fpQtA';

// NYC Borough configurations
const BOROUGH_CENTROIDS = {
    Manhattan: [40.7831, -73.9712],
    Brooklyn: [40.6782, -73.9442],
    Bronx: [40.8448, -73.8648],
    Queens: [40.7282, -73.7949],
    'Staten Island': [40.5795, -74.1502]
};

// Color schemes for boroughs
const BOROUGH_COLORS = {
    Manhattan: '#3182CE',
    Brooklyn: '#F59E0B',
    Bronx: '#10B981',
    Queens: '#6366F1', 
    'Staten Island': '#EC4899'
};

// Weather station mapping
const WEATHER_STATIONS = {
    Manhattan: 'USW00094728',
    Brooklyn: 'USW00094777',
    Bronx: 'USW00014727',
    Queens: 'USW00014734',
    'Staten Island': 'USW00014736'
};

// Dataset color coding
const DATASET_COLORS = {
    calls311: '#3B82F6',
    transit: '#10B981',
    taxi: '#F59E0B',
    events: '#8B5CF6',
    weather: '#A855F7'
};

// NYC Geojson source
const GEOJSON_URL = 'https://raw.githubusercontent.com/codeforgermany/click_that_hood/main/public/data/new-york-city-boroughs.geojson';

// API endpoints
const API_CONFIG = {
    weatherProxy: 'http://localhost:3000/noaa',
    geocodingEndpoint: 'https://api.mapbox.com/geocoding/v5/mapbox.places/'
};

// Default temporal aggregation settings
const TIME_AGGREGATION = {
    hourly: {
        format: 'HH:mm',
        labelFormat: 'ha',
        buckets: 24
    },
    daily: {
        format: 'YYYY-MM-DD',
        labelFormat: 'MMM D',
        buckets: 31
    },
    weekly: {
        format: 'YYYY-[W]WW',
        labelFormat: 'MMM D',
        buckets: 52
    },
    monthly: {
        format: 'YYYY-MM',
        labelFormat: 'MMM YYYY',
        buckets: 12
    }
};

// Chart configuration defaults
const CHART_DEFAULTS = {
    responsive: true,
    maintainAspectRatio: false,
    animation: {
        duration: 1000
    },
    scales: {
        y: {
            beginAtZero: true,
            ticks: {
                font: { size: 10 }
            }
        },
        x: {
            ticks: {
                font: { size: 10 }
            }
        }
    },
    plugins: {
        legend: {
            position: 'top',
            labels: {
                boxWidth: 12,
                font: { size: 10 }
            }
        },
        tooltip: {
            backgroundColor: 'rgba(255, 255, 255, 0.9)',
            titleColor: '#333',
            bodyColor: '#333',
            borderColor: '#ddd',
            borderWidth: 1,
            padding: 10,
            displayColors: true,
            titleFont: { size: 12, weight: 'bold' },
            bodyFont: { size: 11 }
        }
    }
};

// Zone ID to borough mapping for TLC data
const ZONE_TO_BOROUGH = {
    // Manhattan (1-143)
    1: 'Manhattan', 2: 'Manhattan', 3: 'Manhattan', 4: 'Manhattan', 5: 'Manhattan',
    6: 'Manhattan', 7: 'Manhattan', 8: 'Manhattan', 9: 'Manhattan', 10: 'Manhattan',
    11: 'Manhattan', 12: 'Manhattan', 13: 'Manhattan', 14: 'Manhattan', 15: 'Manhattan',
    // ... add more as needed for your dataset
    
    // Bronx (144-200)
    144: 'Bronx', 145: 'Bronx', 146: 'Bronx', 147: 'Bronx', 148: 'Bronx',
    // ... add more as needed
    
    // Brooklyn (201-263)
    201: 'Brooklyn', 202: 'Brooklyn', 203: 'Brooklyn', 204: 'Brooklyn', 205: 'Brooklyn',
    // ... add more as needed
    
    // Queens (264-324)
    264: 'Queens', 265: 'Queens', 266: 'Queens', 267: 'Queens', 268: 'Queens',
    // ... add more as needed
    
    // Staten Island (325-344)
    325: 'Staten Island', 326: 'Staten Island', 327: 'Staten Island', 328: 'Staten Island',
    // ... add more as needed
};