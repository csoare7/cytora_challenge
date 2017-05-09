
# coding: utf-8

# In[68]:

from google.cloud import storage
from google.oauth2 import service_account
from opencage.geocoder import OpenCageGeocode
import googlemaps
import google.auth
import json
import spacy 
import time

PROJECT = "cytora-interviews"
CREDENTIALS = "cytora-interview-service-account.json"
BUCKET = "cytora-interview-data"

with open('keys.json') as json_data:
    keys = json.load(json_data)

GOOGLEMAPS_API_KEY = keys["gmaps_api_key"]
gmaps = googlemaps.Client(key=GOOGLEMAPS_API_KEY)
nlp = spacy.load('en')


# In[60]:

# https://medium.com/google-cloud/simple-google-api-auth-samples-for-service-accounts-installed-application-and-appengine-da30ee4648
def connect():
  credentials = service_account.Credentials.from_service_account_file(CREDENTIALS)
  if credentials.requires_scopes:
    credentials = credentials.with_scopes(['https://www.googleapis.com/auth/devstorage.read_write'])
    try:
      return storage.Client(project=PROJECT, credentials=credentials)
    except Exception as e:
      raise e
client = connect()


# In[64]:

articles = []
def get_blobs(client):
  try:
    bucket = client.get_bucket(BUCKET)
    if bucket:
      blobs = bucket.list_blobs()
      for blob in blobs:
        articles.append(json.loads(blob.download_as_string().decode("utf-8")))
  except Exception as e:
    raise e
get_blobs(client)


# In[95]:

def get_personal_ents(text):
    personal_ents = []
    for ent in text.ents:
        if ent.label_ == 'PERSON' and ent.text not in personal_ents:
            personal_ents.append(ent.text)
    return personal_ents

def get_organisations(text):
    organisations = []
    for ent in text.ents:
        if ent.label_ == 'ORG' and ent.text not in organisations:
            organisations.append(ent.text)
    return organisations

def get_geo_locations(text):
    locations = []
    location_names = []
    for ent in text.ents:
        loc_name = ent.text
        if ent.label_ == 'GPE' and loc_name not in location_names:
            location_names.append(loc_name)
            locations.append({"name": loc_name, "location": get_geocode_from_location(loc_name)})
    return locations  

# need to keep cache of existing locations to avoid double requests & keep quota low 
# cached_locations = {"name1": {"lat": 0, "long": 0}, ...}
cached_locations = {}
def get_geocode_from_location(loc_name):
    location = {"lat": 0, "long": 0}
    if loc_name in cached_locations:
        # read cached data if exists already
        location = cached_locations[loc_name]
    else:
        # fetch geoloc data from Google API
        results = gmaps.geocode(loc_name)
        if len(results) > 0:
            location["lat"] = results[0]["geometry"]["location"]["lat"]
            location["long"] = results[0]["geometry"]["location"]["lng"]
            # cache location data
            cached_locations[loc_name] = location
    return location

for article in articles:
    processed_content = nlp(article["content"])
    article["personal_ents"] = get_personal_ents(processed_content)
    article["organisations"] = get_organisations(processed_content)
    article["geo_locations"] = get_geo_locations(processed_content)
    time.sleep(100)


# In[96]:



