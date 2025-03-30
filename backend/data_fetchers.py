# data_fetchers.py
import requests
import pandas as pd
from sodapy import Socrata
import json
from datetime import datetime, timedelta

class NYC311Fetcher:
    def __init__(self, app_token):
        self.app_token = app_token
        self.base_url = "data.cityofnewyork.us"
        self.resource_id = "erm2-nwe9"
        
    def fetch_data(self, start_date, end_date, limit=10000):
        client = Socrata(self.base_url, self.app_token)
        
        # Format query
        query = f"created_date >= '{start_date}' AND created_date <= '{end_date}'"
        
        # Fields to select
        select = "created_date, complaint_type, descriptor, incident_zip, latitude, longitude, location"
        
        results = client.get(self.resource_id, 
                            where=query, 
                            select=select, 
                            limit=limit)
        
        return pd.DataFrame.from_records(results)
    
class MTATurnstileFetcher:
    def __init__(self, app_token):
        self.app_token = app_token
        # Add implementation for MTA turnstile data
        
class TLCTripFetcher:
    def __init__(self):
        # Add implementation for TLC trip data
        pass
        
class WeatherFetcher:
    def __init__(self, app_token):
        self.app_token = app_token
        self.base_url = "data.cityofnewyork.us"
        self.resource_id = "5gde-fmj3"
        
    def fetch_data(self, start_date, end_date, limit=10000):
        client = Socrata(self.base_url, self.app_token)
        
        # Format query
        query = f"date >= '{start_date}' AND date <= '{end_date}'"
        
        results = client.get(self.resource_id, 
                            where=query,
                            limit=limit)
        
        return pd.DataFrame.from_records(results)
        
class EventsFetcher:
    def __init__(self, app_token):
        self.app_token = app_token
        self.base_url = "data.cityofnewyork.us"
        self.resource_id = "8end-qv57"
        
    def fetch_data(self, start_date, end_date, limit=10000):
        client = Socrata(self.base_url, self.app_token)
        
        # Format query for events
        query = f"start_date_time >= '{start_date}' AND start_date_time <= '{end_date}'"
        
        results = client.get(self.resource_id, 
                            where=query,
                            limit=limit)
        
        return pd.DataFrame.from_records(results)