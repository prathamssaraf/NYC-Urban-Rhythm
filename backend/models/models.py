from flask_sqlalchemy import SQLAlchemy
from geoalchemy2 import Geometry
from datetime import datetime

db = SQLAlchemy()

class Neighborhood(db.Model):
    __tablename__ = 'neighborhoods'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    borough = db.Column(db.String(50), nullable=False)
    geometry = db.Column(Geometry('MULTIPOLYGON'))
    
    def __repr__(self):
        return f'<Neighborhood {self.name}>'

class NYC311Call(db.Model):
    __tablename__ = 'nyc_311_calls'
    
    id = db.Column(db.Integer, primary_key=True)
    created_date = db.Column(db.DateTime, nullable=False)
    complaint_type = db.Column(db.String(255), nullable=False)
    descriptor = db.Column(db.String(255))
    incident_zip = db.Column(db.String(10))
    geometry = db.Column(Geometry('POINT'))
    neighborhood_id = db.Column(db.Integer, db.ForeignKey('neighborhoods.id'))
    
    neighborhood = db.relationship('Neighborhood', backref='calls')
    
    def __repr__(self):
        return f'<NYC311Call {self.id}: {self.complaint_type}>'

class MTATurnstile(db.Model):
    __tablename__ = 'mta_turnstile'
    
    id = db.Column(db.Integer, primary_key=True)
    station_name = db.Column(db.String(255), nullable=False)
    datetime = db.Column(db.DateTime, nullable=False)
    entries = db.Column(db.Integer, nullable=False)
    exits = db.Column(db.Integer, nullable=False)
    geometry = db.Column(Geometry('POINT'))
    neighborhood_id = db.Column(db.Integer, db.ForeignKey('neighborhoods.id'))
    
    neighborhood = db.relationship('Neighborhood', backref='turnstiles')
    
    def __repr__(self):
        return f'<MTATurnstile {self.station_name}: {self.datetime}>'

class TLCTrip(db.Model):
    __tablename__ = 'tlc_trips'
    
    id = db.Column(db.Integer, primary_key=True)
    pickup_datetime = db.Column(db.DateTime, nullable=False)
    dropoff_datetime = db.Column(db.DateTime, nullable=False)
    passenger_count = db.Column(db.Integer)
    trip_distance = db.Column(db.Float)
    pickup_location = db.Column(Geometry('POINT'))
    dropoff_location = db.Column(Geometry('POINT'))
    pickup_neighborhood_id = db.Column(db.Integer, db.ForeignKey('neighborhoods.id'))
    dropoff_neighborhood_id = db.Column(db.Integer, db.ForeignKey('neighborhoods.id'))
    
    pickup_neighborhood = db.relationship('Neighborhood', foreign_keys=[pickup_neighborhood_id])
    dropoff_neighborhood = db.relationship('Neighborhood', foreign_keys=[dropoff_neighborhood_id])
    
    def __repr__(self):
        return f'<TLCTrip {self.id}: {self.pickup_datetime}>'

class Weather(db.Model):
    __tablename__ = 'weather'
    
    id = db.Column(db.Integer, primary_key=True)
    datetime = db.Column(db.DateTime, nullable=False)
    temperature = db.Column(db.Float)
    precipitation = db.Column(db.Float)
    humidity = db.Column(db.Float)
    wind_speed = db.Column(db.Float)
    weather_condition = db.Column(db.String(50))
    
    def __repr__(self):
        return f'<Weather {self.datetime}: {self.weather_condition}>'

class Event(db.Model):
    __tablename__ = 'events'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    start_datetime = db.Column(db.DateTime, nullable=False)
    end_datetime = db.Column(db.DateTime, nullable=False)
    category = db.Column(db.String(100))
    location = db.Column(Geometry('POINT'))
    neighborhood_id = db.Column(db.Integer, db.ForeignKey('neighborhoods.id'))
    
    neighborhood = db.relationship('Neighborhood', backref='events')
    
    def __repr__(self):
        return f'<Event {self.name}: {self.start_datetime}>'