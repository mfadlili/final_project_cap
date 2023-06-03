import requests
from datetime import datetime, timedelta, timezone
import pandas as pd

yesterday = str(datetime.now(timezone.utc) - timedelta(days=1)).split()[0]
linux_path = '/home/fadlil/property/'

url = ["https://us-real-estate.p.rapidapi.com/v2/for-sale", "https://us-real-estate.p.rapidapi.com/v2/for-rent"]

state_city = {"state": ["NY", "CA", "TX", "FL", "PA", "CA"], "city": ["New York", "Los Angeles", "Houston", "Miami", "Philadelphia", "San Diego"]}
headers = {
	"X-RapidAPI-Key": "",
	"X-RapidAPI-Host": "us-real-estate.p.rapidapi.com"
}

def get_data(url, headers, state_city):
    result = []
    for link in url:
        for i in range(len(state_city["state"])):
            params = {"offset":"0","limit":"200","state_code":state_city["state"][i], "city":state_city["city"][i],"sort":"newest"}
            response = requests.get(link, headers=headers, params=params)
            if response.status_code == 200:
                result.append(response.json())
    
    tampung = []
    for city_data in result:
        for data in city_data['data']['home_search']['results']:
            try:
                if yesterday in str(data['list_date']):
                    dict_tampung = {}

                    dict_tampung['property_id'] = data['property_id']

                    try:
                        dict_tampung['list_date'] = data['list_date']
                    except:
                        dict_tampung['list_date'] = None

                    try:
                        dict_tampung['list_price'] = data['list_price']
                    except:
                        dict_tampung['list_price'] = None

                    try:
                        dict_tampung['feature'] = data['tags']
                    except:
                        dict_tampung['feature'] = None

                    try:
                        dict_tampung['tax_id'] = data['tax_record']['public_record_id']
                    except:
                        dict_tampung['tax_id'] = None

                    try:
                        dict_tampung['photo'] = data['primary_photo']['href']
                    except:
                        dict_tampung['photo'] = None

                    dict_tampung['line'] = data['location']['address']['line']
                    dict_tampung['status'] = data['status']

                    try:
                        dict_tampung['broker'] = data['branding'][0]['name']
                    except:
                        dict_tampung['broker'] = None
                    
                    try:
                        dict_tampung['year_built'] = data['description']['year_built']
                    except:
                        dict_tampung['year_built'] = None

                    try:
                        dict_tampung['total_sqft'] = data['description']['lot_sqft']
                    except:
                        dict_tampung['total_sqft'] = None

                    try:
                        dict_tampung['building_sqft'] = data['description']['sqft']
                    except:
                        dict_tampung['building_sqft'] = None

                    try:
                        dict_tampung['baths'] = data['description']['baths']
                    except:
                        dict_tampung['baths'] = None
                    
                    try:
                        dict_tampung['state'] = data['location']['address']['state']
                    except:
                        dict_tampung['state'] = None
                    
                    try:
                        dict_tampung['county'] = data['location']['county']['name']
                    except:
                        dict_tampung['county'] = None
                    
                    try:
                        dict_tampung['city'] = data['location']['address']['city']
                    except:
                        dict_tampung['city'] = None
                    
                    try:
                        dict_tampung['postal_code'] = data['location']['address']['postal_code']
                    except:
                        dict_tampung['postal_code'] = None

                    try:
                        dict_tampung['stories'] = data['description']['stories']
                    except:
                        dict_tampung['stories'] = None
                    
                    try:
                        dict_tampung['beds'] = data['description']['beds']
                    except:
                        dict_tampung['beds'] = None
                    
                    try:
                        dict_tampung['garage'] = data['description']['garage']
                    except:
                        dict_tampung['garage'] = None
                    
                    try:
                        dict_tampung['type'] = data['description']['type']
                    except:
                        dict_tampung['type'] = None

                    try:
                        dict_tampung['name'] = data['description']['name']
                    except:
                        dict_tampung['name'] = None

                    tampung.append(dict_tampung)
            except:
                pass
    df = pd.DataFrame(tampung)
    df.to_parquet(linux_path+yesterday+'.parquet')

get_data(url, headers, state_city)