# %%
from flask import Flask,request, jsonify
# Librerias de Google para acceder a GCP
from google.cloud import storage
from google.oauth2 import service_account
# Librerias de Mongo para acceder al cluster
from pymongo import MongoClient
import pymongo
import datetime
import dotenv
import os

#%%
# direccion del cluster de mongodb
uri = os.getenv('MONGO_URI')
#==== accesos de google cloud storage ====#
key_json_filename = 'nds-proyecto-123-credentials.json'
credentials = service_account.Credentials.from_service_account_file(
    key_json_filename,
)
gcs_client = storage.Client(
    project = credentials.project_id,
    credentials = credentials
)
BUCKET_NAME = fr"bucket-nds-stt-endpoint"
bucket = gcs_client.get_bucket(BUCKET_NAME)

#==== accesos de Mongo ====#

#gs://bucket-nds-stt-endpoint/audios_en/audio1.wav

client = MongoClient(uri,
                    tls=True,
                    tlsCertificateKeyFile="mongo_db_certificate.pem")
db = client['backend-endpoint']
collection = db['texto-audio']

#%%
def TXT_read(filename=""):                            #Asegurar la funcion de leer archivo .txt
    blob = bucket.blob(filename)
    file_as_string = blob.download_as_string()
    return "file downloaded"#data, sample_rate

#GCP upload
def upload_file(filename="", data=None):
    blob = bucket.blob(filename)
    blob.upload_from_string(data)

def getCountDocuments():
    try:
        return collection.count_documents({})
    except:
        return f"Error while executing"

#Mongo Upload
def insertDocument(values):
    try:
        x = collection.insert_one(values)
        r_id = x.inserted_id
        return fr'{r_id}'
    except pymongo.errors.DuplicateKeyError:
        return f"Llave duplicada"
    
app = Flask(__name__)

@app.route('/', methods=["GET"])
def get_tts():
    query_parameters = request.args

    id = query_parameters.get('id')
    #lang = query_parameters.get('language')
    lang = os.getenv('LAN')

    if lang == 'spanish':                           #Corregir formato de lang en .env
        folder = 'audios_es/'                          #revisar formato en GCP
    elif lang == 'english':
        folder = 'audios_en/'

    file = fr"{folder}audio{id}.wav"

    #gs://bucket-nds-stt-endpoint/audios_es/audio1.wav
    data, sample_rate = TXT_read(file)
    #obtengo el nombre del audio reconocido y el transcript en texto; guardo el transcript
    # for fname, transcript in zip([file], asr_model.transcribe(paths2audio_files=[file])):
    #     print(f"Audio {fname} reconoció el siguiente texto:", transcript)
    
    transcript = "Hola Mundo"
    filename = fr'transcript/input_{id}'
    val = {'user_id': id,                                     #Hardcodeado lol
           'uploadDate': datetime.datetime.utcnow(),
           'language': lang,
           'filename': fr'gs://{BUCKET_NAME}/{filename}.txt', #Cambiar .txt por .wav, tener el bucket correcto
           'textIn': transcript,
           'stage': 2}
    
    upload_file(filename, transcript)
    ret_id = insertDocument(val)

    return jsonify([transcript, ret_id])

app.run(host = '0.0.0.0', debug=True, port=8080)                    #Ver que no colapse todo por usar Flask