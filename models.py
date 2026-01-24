from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()



class Radiosonda(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ser = db.Column(db.String(50))
    frame = db.Column(db.Integer)  
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    alt = db.Column(db.Float)
    speed = db.Column(db.Float)
    dir = db.Column(db.Float)
    type = db.Column(db.String(20))
    time = db.Column(db.Integer)  
    sats = db.Column(db.Integer)
    freq = db.Column(db.Float)
    rssi = db.Column(db.Float)
    vs = db.Column(db.Float)
    hs = db.Column(db.Float)
    climb = db.Column(db.Float)
    temp = db.Column(db.Float)
    humidity = db.Column(db.Float)
    vframe = db.Column(db.Integer)
    launchsite = db.Column(db.String(50))
    batt = db.Column(db.Float)