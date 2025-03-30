/**
 * Weather correlation analysis for NYC Urban Rhythm
 */

// Store weather data by borough
let weatherByBorough = {};

/**
 * Fetch weather data from NOAA API through proxy
 * @param {string} startDate - Start date in YYYY-MM-DD format
 * @param {string} endDate - End date in YYYY-MM-DD format
 * @returns {Object} - Weather data indexed by date
 */
async function fetchWeatherData(startDate, endDate) {
    showLoading('Fetching weather data...');
    
    try {
        // Initialize weather data
        const weatherData = {};
        
        // Convert string dates to Date objects
        const start = new Date(startDate);
        const end = new Date(endDate);
        
        // Limit to one week to avoid too many requests
        const limitedEnd = new Date(start);
        limitedEnd.setDate(limitedEnd.getDate() + 7);
        const actualEnd = limitedEnd > end ? end : limitedEnd;
        
        // Fetch weather data for each day
        const currentDate = new Date(start);
        while (currentDate <= actualEnd) {
            const dateStr = formatDate(currentDate);
            
            // Fetch for each borough's weather station
            const boroughPromises = Object.entries(WEATHER_STATIONS).map(async ([borough, stationId]) => {
                try {
                    const data = await fetchNOAAStation(stationId, dateStr);
                    
                    if (data) {
                        if (!weatherByBorough[borough]) {
                            weatherByBorough[borough] = {};
                        }
                        weatherByBorough[borough][dateStr] = data;
                        
                        // Add to master weather data if not already present
                        if (!weatherData[dateStr]) {
                            weatherData[dateStr] = data;
                        }
                    }
                } catch (e) {
                    console.error(`Error fetching weather for ${borough} on ${dateStr}:`, e);
                }
            });
            
            await Promise.all(boroughPromises);
            
            // Move to next day
            currentDate.setDate(currentDate.getDate() + 1);
        }
        
        return weatherData;
    } catch (error) {
        console.error('Error fetching weather data:', error);
        updateInfoText('Weather data unavailable. Using 311 data only.');
        return {};
    } finally {
        hideLoading();
    }
}

/**
 * Fetch weather data for a specific station and date
 * @param {string} stationId - NOAA station ID
 * @param {string} date - Date in YYYY-MM-DD format
 * @returns {Object|null} - Weather data or null if error
 */
async function fetchNOAAStation(stationId, date) {
    try {
        const url = `${API_CONFIG.weatherProxy}?station=${stationId}&date=${date}`;
        const response = await fetch(url);
        
        if (!response.ok) return null;
        
        const { results = [] } = await response.json();
        
        return {
            tmax: results.find(r => r.datatype === 'TMAX')?.value || null,
            tmin: results.find(r => r.datatype === 'TMIN')?.value || null,
            prcp: results.find(r => r.datatype === 'PRCP')?.value || 0,
            date
        };
    } catch (error) {
        console.error('Weather fetch error:', error);
        return null;
    }
}

/**
 * Analyze correlation between weather and urban activity
 * @param {Object} weatherData - Weather data by date
 * @returns {Object} - Weather correlation analysis
 */
function analyzeWeatherCorrelation(weatherData) {
    // Store correlation analysis
    const analysis = {
        temperature: {
            overall: null,
            byBorough: {},
            byComplaint: {}
        },
        precipitation: {
            overall: null,
            byBorough: {},
            byComplaint: {}
        }
    };
    
    // Valid boroughs
    const validBoroughs = Object.keys(BOROUGH_CENTROIDS);
    
    // Analyze overall temperature correlation
    const tempPoints = [];
    Object.entries(weatherData).forEach(([date, data]) => {
        if (data.tmax === null || !processedData.calls311.dailyData || !processedData.calls311.dailyData[date]) return;
        
        tempPoints.push({
            temp: data.tmax,
            calls: processedData.calls311.dailyData[date].total
        });
    });
    
    if (tempPoints.length >= 3) {
        const tempX = tempPoints.map(p => p.temp);
        const tempY = tempPoints.map(p => p.calls);
        analysis.temperature.overall = calculateCorrelation(tempX, tempY);
    }
    
    // Analyze overall precipitation correlation
    const precipPoints = [];
    Object.entries(weatherData).forEach(([date, data]) => {
        if (data.prcp === null || !processedData.calls311.dailyData || !processedData.calls311.dailyData[date]) return;
        
        precipPoints.push({
            prcp: data.prcp,
            calls: processedData.calls311.dailyData[date].total
        });
    });
    
    if (precipPoints.length >= 3) {
        const precipX = precipPoints.map(p => p.prcp);
        const precipY = precipPoints.map(p => p.calls);
        analysis.precipitation.overall = calculateCorrelation(precipX, precipY);
    }
    
    // Analyze by borough
    validBoroughs.forEach(borough => {
        // Temperature correlation
        const boroughTempPoints = [];
        Object.entries(weatherData).forEach(([date, data]) => {
            if (data.tmax === null || !processedData.calls311.dailyData || !processedData.calls311.dailyData[date]) return;
            
            boroughTempPoints.push({
                temp: data.tmax,
                calls: processedData.calls311.dailyData[date].byBorough[borough] || 0
            });
        });
        
        if (boroughTempPoints.length >= 3) {
            const boroughTempX = boroughTempPoints.map(p => p.temp);
            const boroughTempY = boroughTempPoints.map(p => p.calls);
            analysis.temperature.byBorough[borough] = calculateCorrelation(boroughTempX, boroughTempY);
        }
        
        // Precipitation correlation
        const boroughPrecipPoints = [];
        Object.entries(weatherData).forEach(([date, data]) => {
            if (data.prcp === null || !processedData.calls311.dailyData || !processedData.calls311.dailyData[date]) return;
            
            boroughPrecipPoints.push({
                prcp: data.prcp,
                calls: processedData.calls311.dailyData[date].byBorough[borough] || 0
            });
        });
        
        if (boroughPrecipPoints.length >= 3) {
            const boroughPrecipX = boroughPrecipPoints.map(p => p.prcp);
            const boroughPrecipY = boroughPrecipPoints.map(p => p.calls);
            analysis.precipitation.byBorough[borough] = calculateCorrelation(boroughPrecipX, boroughPrecipY);
        }
    });
    
    // Analyze by complaint type
    if (processedData.calls311.complaintTypes) {
        // Get top complaint types
        const topComplaints = Object.entries(processedData.calls311.complaintTypes)
            .sort((a, b) => b[1].total - a[1].total)
            .slice(0, 10)
            .map(([complaint]) => complaint);
        
        // Temperature correlation by complaint
        topComplaints.forEach(complaint => {
            const complaintTempData = [];
            
            // Count complaints by date and temperature
            all311Data.forEach(call => {
                if (call.complaint !== complaint) return;
                
                const date = call.parsed_date;
                if (!weatherData[date] || weatherData[date].tmax === null) return;
                
                complaintTempData.push({
                    temp: weatherData[date].tmax,
                    count: 1
                });
            });
            
            // Group by temperature rounded to nearest 5 degrees
            const groupedByTemp = {};
            complaintTempData.forEach(data => {
                const roundedTemp = Math.round(data.temp / 5) * 5;
                if (!groupedByTemp[roundedTemp]) groupedByTemp[roundedTemp] = 0;
                groupedByTemp[roundedTemp] += data.count;
            });
            
            // Calculate correlation if we have enough data points
            const temps = Object.keys(groupedByTemp).map(t => parseInt(t));
            const counts = Object.values(groupedByTemp);
            
            if (temps.length >= 3) {
                analysis.temperature.byComplaint[complaint] = calculateCorrelation(temps, counts);
            }
        });
        
        // Precipitation correlation by complaint
        topComplaints.forEach(complaint => {
            // Count on rainy vs non-rainy days
            let rainyDayCount = 0, rainyDayTotal = 0;
            let dryDayCount = 0, dryDayTotal = 0;
            
            Object.entries(weatherData).forEach(([date, data]) => {
                if (data.prcp === null) return;
                
                const isRainy = data.prcp > 0.1;
                const complaintCount = all311Data.filter(call => 
                    call.parsed_date === date && call.complaint === complaint
                ).length;
                
                if (isRainy) {
                    rainyDayCount++;
                    rainyDayTotal += complaintCount;
                } else {
                    dryDayCount++;
                    dryDayTotal += complaintCount;
                }
            });
            
            // Calculate average per day
            const rainyAvg = rainyDayCount > 0 ? rainyDayTotal / rainyDayCount : 0;
            const dryAvg = dryDayCount > 0 ? dryDayTotal / dryDayCount : 0;
            
            // Calculate ratio
            const ratio = Math.max(rainyAvg, dryAvg) / Math.max(0.1, Math.min(rainyAvg, dryAvg));
            const diff = Math.abs(rainyAvg - dryAvg);
            
            if (rainyDayCount > 0 && dryDayCount > 0) {
                analysis.precipitation.byComplaint[complaint] = {
                    rainyAvg,
                    dryAvg,
                    ratio,
                    diff,
                    moreFrequent: rainyAvg > dryAvg ? 'rainy' : 'dry'
                };
            }
        });
    }
    
    return analysis;
}

/**
 * Find weather-sensitive complaint types
 * @returns {string} - HTML string with findings
 */
function findWeatherSensitiveComplaints() {
    // Temperature ranges for analysis
    const tempRanges = [
        { name: 'Cold', min: -20, max: 40 },
        { name: 'Cool', min: 40, max: 60 },
        { name: 'Mild', min: 60, max: 75 },
        { name: 'Warm', min: 75, max: 85 },
        { name: 'Hot', min: 85, max: 110 }
    ];
    
    // Complaint counters
    const complaintsByTemp = {};
    const complaintsByPrec = { 'Rainy': {}, 'Dry': {} };
    
    // Analyze by temperature
    Object.entries(processedData.weather).forEach(([date, data]) => {
        if (!processedData.calls311.dailyData || !processedData.calls311.dailyData[date]) return;
        
        // Process by temperature
        if (data.tmax !== null) {
            const range = tempRanges.find(r => data.tmax >= r.min && data.tmax < r.max);
            if (range) {
                // Get complaints for this day
                const dayComplaints = {};
                
                all311Data.forEach(call => {
                    if (call.parsed_date === date) {
                        if (!dayComplaints[call.complaint]) dayComplaints[call.complaint] = 0;
                        dayComplaints[call.complaint]++;
                    }
                });
                
                // Add to temperature categories
                Object.entries(dayComplaints).forEach(([complaint, count]) => {
                    if (!complaintsByTemp[complaint]) {
                        complaintsByTemp[complaint] = tempRanges.reduce((acc, r) => {
                            acc[r.name] = { count: 0, days: 0 };
                            return acc;
                        }, {});
                    }
                    
                    complaintsByTemp[complaint][range.name].count += count;
                    complaintsByTemp[complaint][range.name].days += 1;
                });
            }
        }
        
        // Process by precipitation
        if (data.prcp !== null) {
            const isRainy = data.prcp > 0.1;
            const category = isRainy ? 'Rainy' : 'Dry';
            
            all311Data.forEach(call => {
                if (call.parsed_date === date) {
                    if (!complaintsByPrec[category][call.complaint]) {
                        complaintsByPrec[category][call.complaint] = 0;
                    }
                    complaintsByPrec[category][call.complaint]++;
                }
            });
        }
    });
    
    // Find temperature-sensitive complaints
    const tempSensitive = Object.entries(complaintsByTemp)
        .map(([complaint, ranges]) => {
            // Calculate complaint frequency per day for each temperature range
            const frequencies = Object.entries(ranges).map(([range, data]) => {
                return { range, freq: data.days > 0 ? data.count / data.days : 0 };
            });
            
            // Find min and max frequency
            const freqValues = frequencies.map(f => f.freq);
            const minFreq = Math.min(...freqValues);
            const maxFreq = Math.max(...freqValues);
            
            // Calculate the relative spread
            const spread = minFreq > 0 ? maxFreq / minFreq : 0;
            
            return {
                complaint,
                spread,
                frequencies
            };
        })
        .filter(c => c.spread > 1.5 && c.frequencies.some(f => f.freq > 5))
        .sort((a, b) => b.spread - a.spread)
        .slice(0, 5);
    
    // Find precipitation-sensitive complaints
    const precipSensitive = [];
    
    Object.keys(complaintsByPrec.Rainy).forEach(complaint => {
        if (complaintsByPrec.Dry[complaint]) {
            const rainyCount = complaintsByPrec.Rainy[complaint];
            const dryCount = complaintsByPrec.Dry[complaint];
            
            // Get the number of rainy vs dry days
            const rainyDays = Object.entries(processedData.weather)
                .filter(([_, data]) => data.prcp > 0.1)
                .length;
                
            const dryDays = Object.entries(processedData.weather)
                .filter(([_, data]) => data.prcp <= 0.1)
                .length;
            
            // Calculate per-day rate
            const rainyRate = rainyDays > 0 ? rainyCount / rainyDays : 0;
            const dryRate = dryDays > 0 ? dryCount / dryDays : 0;
            
            // Calculate the relative change
            const ratio = Math.max(rainyRate, dryRate) / Math.max(0.1, Math.min(rainyRate, dryRate));
            const diff = Math.abs(rainyRate - dryRate);
            
            if (ratio > 1.3 && diff > 1) {
                precipSensitive.push({
                    complaint,
                    rainyRate,
                    dryRate,
                    ratio,
                    more: rainyRate > dryRate ? 'rainy' : 'dry'
                });
            }
        }
    });
    
    // Sort by ratio
    precipSensitive.sort((a, b) => b.ratio - a.ratio);
    
    // Create HTML output
    let html = '<div class="grid grid-cols-1 md:grid-cols-2 gap-3 text-xs">';
    
    // Temperature findings
    html += '<div>';
    html += '<h4 class="font-medium mb-1">Temperature-Sensitive Complaints:</h4>';
    if (tempSensitive.length === 0) {
        html += '<p>Insufficient data to determine temperature correlation.</p>';
    } else {
        html += '<ul class="list-disc pl-4">';
        tempSensitive.forEach(item => {
            const maxFreq = Math.max(...item.frequencies.map(f => f.freq));
            const maxRange = item.frequencies.find(f => f.freq === maxFreq).range;
            
            html += `<li><span class="font-medium">${item.complaint}</span>: ${Math.round(item.spread * 10) / 10}x more frequent during <span class="font-medium">${maxRange}</span> weather</li>`;
        });
        html += '</ul>';
    }
    html += '</div>';
    
    // Precipitation findings
    html += '<div>';
    html += '<h4 class="font-medium mb-1">Precipitation-Sensitive Complaints:</h4>';
    if (precipSensitive.length === 0) {
        html += '<p>Insufficient data to determine precipitation correlation.</p>';
    } else {
        html += '<ul class="list-disc pl-4">';
        precipSensitive.slice(0, 5).forEach(item => {
            html += `<li><span class="font-medium">${item.complaint}</span>: ${Math.round(item.ratio * 10) / 10}x more frequent during <span class="font-medium">${item.more}</span> days</li>`;
        });
        html += '</ul>';
    }
    html += '</div>';
    
    html += '</div>';
    
    return html;
}