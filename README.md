# 🗺️ NYC Urban Data Dashboard

A comprehensive urban data analysis dashboard for New York City, combining multiple public datasets such as weather, taxi pickups, subway ridership, 311 service requests, and events. The dashboard provides interactive visualizations and insights into how these factors correlate with each other over time and across boroughs.

## 📁 Project Structure

```
nyc-data-dashboard/
├── backend/
│   ├── etl/                # ETL scripts for fetching and processing data
│   │   ├── fetch_weather.py
│   │   ├── fetch_tlc.py     # Taxi pickup data
│   │   ├── fetch_311.py
│   │   ├── fetch_mta.py     # Subway ridership
│   │   └── fetch_events.py
│   └── utils/
├── data/                   # Output CSV files from ETL processes
├── final_front/            # Frontend dashboard (HTML, JS, CSS)
└── README.md
```

## 🚀 Getting Started

### Prerequisites

- Python 3.x
- PostgreSQL (for database storage)
- `.env` file with appropriate credentials:

```
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_NAME=nyc_data
NOAA_API_TOKEN=your_token
```
### Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 🌦️ Fetch Weather Data

Use the following commands to fetch historical weather data for different years:

```bash
python -m backend.etl.fetch_weather --start-date 2020-01-01 --end-date 2020-12-31 --save-csv
python -m backend.etl.fetch_weather --start-date 2021-01-01 --end-date 2021-12-31 --save-csv
python -m backend.etl.fetch_weather --start-date 2022-01-01 --end-date 2022-12-31 --save-csv
python -m backend.etl.fetch_weather --start-date 2023-01-01 --end-date 2023-12-31 --save-csv
python -m backend.etl.fetch_weather --start-date 2024-01-01 --end-date 2024-12-31 --save-csv
python -m backend.etl.fetch_weather --start-date 2025-01-01 --end-date 2025-04-10 --save-csv
```

---

## 🚖 Fetch Taxi Pickup Data (TLC)

Fetch Yellow Taxi trip data month-by-month:

```bash
# Year 2020
python -m backend.etl.fetch_tlc --year 2020 --month 1 --save-csv
python -m backend.etl.fetch_tlc --year 2020 --month 2 --save-csv
...
python -m backend.etl.fetch_tlc --year 2020 --month 12 --save-csv

# Repeat for 2021-2024 similarly
python -m backend.etl.fetch_tlc --year 2021 --month 1 --save-csv
...
python -m backend.etl.fetch_tlc --year 2024 --month 3 --save-csv
```

---

## 🛠️ Fetch 311 Service Requests (Quarterly)

```bash
# 2020
python -m backend.etl.fetch_311 --start-date 2020-01-01 --end-date 2020-03-31 --save-csv
python -m backend.etl.fetch_311 --start-date 2020-04-01 --end-date 2020-06-30 --save-csv
...
# Continue quarterly for 2021–2025
```

---

## 🚇 Fetch MTA Subway Ridership Data (Monthly)

```bash
python -m backend.etl.fetch_mta --start-date 2020-01-01 --end-date 2020-01-31 --save-csv
python -m backend.etl.fetch_mta --start-date 2020-02-01 --end-date 2020-02-29 --save-csv
...
python -m backend.etl.fetch_mta --start-date 2021-01-01 --end-date 2021-01-31 --save-csv
```

---

## 🎉 Fetch NYC Events Data (Quarterly)

```bash
# 2020
python -m backend.etl.fetch_events --start-date 2020-01-01 --end-date 2020-03-31 --save-csv
python -m backend.etl.fetch_events --start-date 2020-04-01 --end-date 2020-06-30 --save-csv
...
# Repeat for 2021 similarly
```

---

## 🌐 Hosting the Dashboard

The frontend visualization is located in the `final_front/` folder. To host the dashboard locally:

```bash
cd final_front
python -m http.server 8000
```

Then open your browser and navigate to:

```
http://localhost:8000
```

> 💡 Tip: For production deployment, consider using a static hosting service like GitHub Pages, Netlify, or Vercel.

---

## 🔍 Features

- Interactive map visualizations powered by **Mapbox** and **D3.js**
- Correlation charts between weather, transit, and complaints
- Time-series graphs of taxi pickups and subway usage
- Borough-wise breakdowns
- Responsive design using **TailwindCSS**

---

## 🧑‍💻 Contributing

Contributions are welcome! Please follow the [contributing guidelines](CONTRIBUTING.md) if you'd like to contribute to this project.

---

## 📄 License

This project is licensed under the MIT License – see the [LICENSE](LICENSE) file for details.

---

## 📬 Feedback & Support

For bugs, feature requests, or general feedback, please open an issue on the GitHub repository.
```