/**
 * Utility functions for NYC Urban Rhythm application
 */

// Date and time formatting helpers
function formatDate(date) {
    return date.toISOString().split('T')[0];
}

function formatDateTime(date) {
    return date.toISOString().replace('T', ' ').substring(0, 19);
}

function parseDateFlexible(raw) {
    if (!raw) return null;
    // Handle different date formats
    const d = new Date(raw.replace(' ', 'T'));
    return isNaN(d) ? null : d;
}

function getTimeOfDay(hour) {
    if (hour >= 5 && hour < 12) return 'Morning';
    if (hour >= 12 && hour < 17) return 'Afternoon';
    if (hour >= 17 && hour < 21) return 'Evening';
    return 'Night';
}

function getDayOfWeekName(dayIndex) {
    return ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'][dayIndex];
}

function getMonthName(monthIndex) {
    return ['January', 'February', 'March', 'April', 'May', 'June', 
            'July', 'August', 'September', 'October', 'November', 'December'][monthIndex];
}

// UI helpers
function showLoading(message) {
    const loadingOverlay = document.getElementById('loading-overlay');
    const loadingText = document.getElementById('loading-text');
    loadingText.textContent = message || 'Loading...';
    loadingOverlay.style.visibility = 'visible';
}

function hideLoading() {
    const loadingOverlay = document.getElementById('loading-overlay');
    loadingOverlay.style.visibility = 'hidden';
}

function updateInfoText(message) {
    const infoText = document.getElementById('info-text');
    infoText.textContent = message;
}

function createPulseEffect(map, lngLat, color = '#3B82F6') {
    // Create a pulse effect on the map at the given coordinates
    const el = document.createElement('div');
    el.className = 'pulse-circle';
    el.style.backgroundColor = `${color}40`; // 40 for 25% opacity
    el.style.borderColor = color;
    
    const marker = new mapboxgl.Marker({ element: el })
        .setLngLat(lngLat)
        .addTo(map);
    
    // Remove after animation completes
    setTimeout(() => {
        marker.remove();
    }, 2000);
}

// Data transformation helpers
function groupByBorough(data, boroughKey = 'borough') {
    return data.reduce((acc, item) => {
        const borough = item[boroughKey];
        if (!acc[borough]) acc[borough] = [];
        acc[borough].push(item);
        return acc;
    }, {});
}

function groupByTimeUnit(data, dateKey, timeUnit = 'day') {
    return data.reduce((acc, item) => {
        let key;
        const date = new Date(item[dateKey]);
        
        switch (timeUnit) {
            case 'hour':
                key = `${formatDate(date)}-${date.getHours()}`;
                break;
            case 'day':
                key = formatDate(date);
                break;
            case 'week':
                // Get ISO week number
                const d = new Date(date);
                d.setHours(0, 0, 0, 0);
                d.setDate(d.getDate() + 3 - (d.getDay() + 6) % 7);
                const week = Math.floor((d.getTime() - new Date(d.getFullYear(), 0, 4).getTime()) / 86400000 / 7) + 1;
                key = `${date.getFullYear()}-W${week.toString().padStart(2, '0')}`;
                break;
            case 'month':
                key = `${date.getFullYear()}-${(date.getMonth() + 1).toString().padStart(2, '0')}`;
                break;
            default:
                key = formatDate(date);
        }
        
        if (!acc[key]) acc[key] = [];
        acc[key].push(item);
        return acc;
    }, {});
}

// Calculate correlation between two arrays
function calculateCorrelation(x, y) {
    if (x.length !== y.length) {
        throw new Error('Arrays must have the same length');
    }
    
    const n = x.length;
    
    // Calculate the sums
    let sumX = 0, sumY = 0, sumXY = 0, sumXX = 0, sumYY = 0;
    for (let i = 0; i < n; i++) {
        sumX += x[i];
        sumY += y[i];
        sumXY += x[i] * y[i];
        sumXX += x[i] * x[i];
        sumYY += y[i] * y[i];
    }
    
    // Calculate Pearson correlation coefficient
    const numerator = n * sumXY - sumX * sumY;
    const denominator = Math.sqrt((n * sumXX - sumX * sumX) * (n * sumYY - sumY * sumY));
    
    if (denominator === 0) return 0;
    return numerator / denominator;
}

// Distance calculation (Haversine formula)
function calculateDistance(lat1, lon1, lat2, lon2) {
    const R = 6371e3; // Earth radius in meters
    const φ1 = lat1 * Math.PI / 180;
    const φ2 = lat2 * Math.PI / 180;
    const Δφ = (lat2 - lat1) * Math.PI / 180;
    const Δλ = (lon2 - lon1) * Math.PI / 180;

    const a = Math.sin(Δφ/2) * Math.sin(Δφ/2) +
              Math.cos(φ1) * Math.cos(φ2) *
              Math.sin(Δλ/2) * Math.sin(Δλ/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));

    return R * c; // Distance in meters
}

// Find nearby items within a radius in meters
function findNearbyItems(lat, lon, items, radiusMeters = 500) {
    return items.filter(item => {
        if (!item.latitude || !item.longitude) return false;
        const distance = calculateDistance(
            lat, lon, 
            parseFloat(item.latitude), 
            parseFloat(item.longitude)
        );
        return distance <= radiusMeters;
    });
}

// Format large numbers with commas
function formatNumber(num) {
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// Get a deterministic color based on a string
function stringToColor(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        hash = str.charCodeAt(i) + ((hash << 5) - hash);
    }
    
    let color = '#';
    for (let i = 0; i < 3; i++) {
        const value = (hash >> (i * 8)) & 0xFF;
        color += ('00' + value.toString(16)).substr(-2);
    }
    
    return color;
}

// Create a lookup object from array
function createLookup(array, keyField) {
    return array.reduce((acc, item) => {
        acc[item[keyField]] = item;
        return acc;
    }, {});
}

// Filter datasets by date range
function filterByDateRange(data, dateField, startDate, endDate) {
    return data.filter(item => {
        const date = item[dateField];
        return date >= startDate && date <= endDate;
    });
}

// Get location ID from coordinates
async function getLocationFromLatLng(lat, lng) {
    try {
        const url = `${API_CONFIG.geocodingEndpoint}${lng},${lat}.json?access_token=${MAPBOX_ACCESS_TOKEN}&types=neighborhood,locality,place`;
        const response = await fetch(url);
        const data = await response.json();
        
        if (data.features && data.features.length > 0) {
            return data.features[0];
        }
        return null;
    } catch (error) {
        console.error('Geocoding error:', error);
        return null;
    }
}

// Create marker element
function createMarkerElement(type, size = 12) {
    const el = document.createElement('div');
    el.className = `marker-${type}`;
    el.style.width = `${size}px`;
    el.style.height = `${size}px`;
    return el;
}

// Generate an HTML tooltip
function generateTooltip(title, content) {
    return `
        <div class="font-medium text-sm">${title}</div>
        <div class="text-xs">${content}</div>
    `;
}