// server.js
import express from 'express';            // Express framework :contentReference[oaicite:3]{index=3}
import fetch  from 'node-fetch';          // HTTP client API :contentReference[oaicite:4]{index=4}

const app = express();
const PORT = 3000;
const NOAA_TOKEN = 'ldwKQqSTQrzXVQyfeVprBpqjqZttygMD';

// Enable CORS for all routes so your browser can call /noaa
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');            // allow any origin :contentReference[oaicite:5]{index=5}
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  next();
});

// Proxy route: forwards to NOAA CDO API and returns JSON
app.get('/noaa', async (req, res) => {                         // define a GET route :contentReference[oaicite:6]{index=6}
  const { station, date } = req.query;
  const params = new URLSearchParams({
    datasetid:  'GHCND',
    stationid:  `GHCND:${station}`,
    startdate:  date, enddate: date,
    datatypeid: 'TMAX,TMIN,PRCP',
    limit:      '1000'
  });

  try {
    const apiRes = await fetch(
      `https://www.ncei.noaa.gov/cdo-web/api/v2/data?${params}`,
      { headers: { token: NOAA_TOKEN } }                   // include your NOAA token :contentReference[oaicite:7]{index=7}
    );

    if (!apiRes.ok) {
      return res.status(apiRes.status).send(await apiRes.text());
    }
    const json = await apiRes.json();
    res.json(json);
  } catch (err) {
    console.error('Proxy error:', err);
    res.status(500).send('Server error');
  }
});

app.listen(PORT, () => {
  console.log(`Proxy server running on http://localhost:${PORT}`);  // server start :contentReference[oaicite:8]{index=8}
});
