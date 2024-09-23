from waitress import serve
from flask import Flask, request, make_response
import requests, json, uuid, random, os
import urllib3
from datetime import datetime
import base64
import gc, string, time
import tracemalloc
from requests.auth import HTTPProxyAuth
import gzip
from random import choice
from http.cookies import SimpleCookie
import urllib
from urllib.parse import urlparse
from styler import envDev, envProd, alertprod
from radiopopular import getProductData as getRadiopopularData
from manomano import getProductData as getManomano
from mediamarket import getProductData as getMediamarket
from amazon import getProductData as getAmazon
from radiopopular import getUrl as getRadiopopularUrl
from ebay_de import getProductData as getebay_deUrl
from mediamarketde import getProductData as getMediamarketde
from amazonde import getProductData as getamazon_deUrl
from manomanode import getProductData as getmanomano_deUrl
from google_ import getProductData as getGoogle
from idealode import getProductData as getIdealo
from dotenv.main import load_dotenv

# Disable warnings for insecure requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables
load_dotenv()
env = os.environ.get('environment', None)

# Print environment settings based on the environment
print(envDev()) if env == "dev" else (print(envProd()), print(alertprod()))

# Initialize the Flask app
app = Flask(__name__)

@app.route('/crawl', methods=['POST'])
def crawl():
    """
    API endpoint to handle crawling requests for different domains.
    It accepts a JSON payload, fetches data, and returns a compressed response.
    """
    try:
        # Get the JSON data from the request
        request_data = request.get_json()
        proxy = request_data.get('proxy', '')
        domain = request_data.get('Website', '')
        url = request_data.get('Url', '')
        apiKey = request_data.get('apikey', '')

        # Validate API key
        if apiKey == "ab980bfh-9066-59y6-6w00-jsk311a1ddc2":
            if url and domain:
                # Route based on the domain and fetch the appropriate data
                if domain == "radiopopular":
                    response = getRadiopopularData(request_data)
                elif domain == "amazon":
                    response = getAmazon(request_data)
                elif domain == "mediamarket":
                    response = getMediamarket(request_data)
                elif domain == "mediamarketde":
                    response = getMediamarketde(request_data)
                elif domain == "manomano":
                    response = getManomano(request_data)
                elif domain == "ebay_de":
                    response = getebay_deUrl(request_data)
                elif domain == "amazon_de":
                    response = getamazon_deUrl(request_data)
                elif domain == "manomano_de":
                    response = getmanomano_deUrl(request_data)
                elif domain == "google_de":
                    response = getGoogle(request_data)
                elif domain == "google_at":
                    response = getGoogle(request_data)
                elif domain == "idealo_de":
                    response = getIdealo(request_data)
                else:
                    gc.collect()
                    return {"statusCode": 400, "statusDescription": "200 OK", "isBase64Encoded": False, 'body': {'error': 'Domain not allowed'}}

                # Compress and return the response
                gc.collect()
                content = gzip.compress(json.dumps(response).encode('utf8'), 5)
                response = make_response(content)
                response.headers['Content-length'] = len(content)
                response.headers['Content-Encoding'] = 'gzip'
                return response

            else:
                gc.collect()
                return {"statusCode": 400, "statusDescription": "200 OK", "isBase64Encoded": False, 'body': {'error': 'Missing parameters'}}
        else:
            gc.collect()
            return {"statusCode": 401, "statusDescription": "200 OK", "isBase64Encoded": False, 'body': {'error': 'Invalid Authentication Key'}}
    except Exception as ex:
        gc.collect()
        return {"statusCode": 502, "statusDescription": "200 OK", "isBase64Encoded": False, 'body': {'error': str(ex)}}

@app.route('/eansearch', methods=['POST'])
def eansearch():
    """
    API endpoint to search by EAN.
    Handles domain-specific requests and returns compressed data.
    """
    try:
        # Get the JSON data from the request
        request_data = request.get_json()
        domain = request_data.get('Website', '')

        # Route based on the domain and fetch the appropriate data
        if domain == "radiopopular":
            response = getRadiopopularData(request_data)
        elif domain == "amazon":
            response = getAmazon(request_data)
        elif domain == "mediamarket":
            response = getMediamarket(request_data)
        elif domain == "manomano":
            response = getManomano(request_data)
        else:
            gc.collect()
            return {"statusCode": 400, "statusDescription": "200 OK", "isBase64Encoded": False, 'body': {'error': 'Domain not allowed'}}

        # Compress and return the response
        gc.collect()
        content = gzip.compress(json.dumps(response).encode('utf8'), 5)
        response = make_response(content)
        response.headers['Content-length'] = len(content)
        response.headers['Content-Encoding'] = 'gzip'
        return response

    except Exception as ex:
        gc.collect()
        return {"statusCode": 502, "statusDescription": "200 OK", "isBase64Encoded": False, 'body': {'error': str(ex)}}

@app.route('/eantosku', methods=['POST'])
def eantosku():
    """
    API endpoint to convert EAN to SKU.
    Handles domain-specific EAN-to-SKU conversions.
    """
    try:
        # Get the JSON data from the request
        request_data = request.get_json()
        domain = request_data.get('domain', '')
        ean = request_data.get('ean', '')
        apiKey = request_data.get('apikey', '')

        # Validate API key
        if apiKey == "ab980bfh-9066-59y6-6w00-jsk311a1ddc2":
            if ean and domain:
                # Route based on the domain and fetch the SKU
                if domain == "radiopopular":
                    response = getRadiopopularUrl(request_data)
                else:
                    gc.collect()
                    return {"statusCode": 400, "statusDescription": "200 OK", "isBase64Encoded": False, 'body': {'error': 'Domain not allowed'}}

                # Compress and return the response
                gc.collect()
                content = gzip.compress(json.dumps(response).encode('utf8'), 5)
                response = make_response(content)
                response.headers['Content-length'] = len(content)
                response.headers['Content-Encoding'] = 'gzip'
                return response

            else:
                gc.collect()
                return {"statusCode": 400, "statusDescription": "200 OK", "isBase64Encoded": False, 'body': {'error': 'Missing parameters'}}
        else:
            gc.collect()
            return {"statusCode": 401, "statusDescription": "200 OK", "isBase64Encoded": False, 'body': {'error': 'Invalid Authentication Key'}}

    except Exception as ex:
        gc.collect()
        return {"statusCode": 502, "statusDescription": "200 OK", "isBase64Encoded": False, 'body': {'error': str(ex)}}

@app.route('/', methods=['GET'])
def index():
    """
    Health check endpoint for checking the API status.
    """
    try:
        # Return appropriate message based on environment
        if env == "dev":
            return "Health check !! Dev environment"
        else:
            return "Health check !! Prod environment"
    except Exception as ex:
        return {'error': str(ex)}, 200

if __name__ == '__main__':
    # Start the server using Waitress on port 5000
    serve(app, host='0.0.0.0', port='5000')
