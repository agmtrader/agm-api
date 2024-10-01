import os

class Config:
    DEBUG = True
    HOST = '0.0.0.0'
    PORT = 5002
    JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY')