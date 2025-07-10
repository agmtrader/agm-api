from src.utils.connectors.firebase import Firebase
from src.utils.connectors.mongodb import MongoDB
from src.utils.connectors.supabase import db

firebase = Firebase()
mongodb = MongoDB()

risk_profiles = firebase.read('db/clients/risk_profiles', {})
print(len(risk_profiles))