
# coding: utf-8

# In[15]:

from google.cloud import storage
from google.oauth2 import service_account
import google.auth
import json
import spacy 

nlp = spacy.load('en')

PROJECT = "cytora-interviews"
CREDENTIALS = "cytora-interview-service-account.json"
BUCKET = "cytora-interview-data"
DOWNLOAD_TO = "data/"


# In[16]:

# https://medium.com/google-cloud/simple-google-api-auth-samples-for-service-accounts-installed-application-and-appengine-da30ee4648
def connect():
  credentials = service_account.Credentials.from_service_account_file(CREDENTIALS)
  if credentials.requires_scopes:
    credentials = credentials.with_scopes(['https://www.googleapis.com/auth/devstorage.read_write'])
    try:
      return storage.Client(credentials=credentials)
    except Exception as e:
      raise e

client = connect()


# In[17]:

articles = []
def get_blobs(client):
  try:
    bucket = client.get_bucket(BUCKET)
    if bucket:
      blobs = bucket.list_blobs()
      for blob in blobs:
        articles.append(json.loads(blob.download_as_string().decode("utf-8")))
        break
  except Exception as e:
    raise e
get_blobs(client)

print(articles)


# In[37]:

def get_personal_ents(text):
    personal_ents = []
    for ent in text.ents:
        if ent.label_ == 'PERSON':
            personal_ents.append(ent)
    return personal_ents

def get_organisations(text):
    organisations = []
    for ent in text.ents:
        if ent.label_ == 'ORG':
            organisations.append(ent)
    return organisations

def get_geo_locations(text):
    locations = []
    for ent in text.ents:
        if ent.label_ == 'GPE':
            locations.append(ent)
    return locations  

for article in articles:
    processed_content = nlp(article["content"])
    article["personal_ents"] = get_personal_ents(processed_content)
    article["organisations"] = get_organisations(processed_content)
    article["geo_locations"] = get_geo_locations(processed_content)
    
print(articles)



# In[ ]:



