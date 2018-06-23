# -*- coding: utf-8 -*-
"""
Maddie Zug
June 23 2018

Based on the Yelp Fusion API code sample.

This program uses the Yelp Fusion Search API
to query for businesses by a search term and location,
and the Business API to query additional information about the top result
from the search query. It then formats the information in 
a csv that is compatible with import into Salesforce.

Please refer to http://www.yelp.com/developers/v3/documentation for the API
documentation.

This program requires the Python requests library, which you can install via:
`pip install -r requirements.txt`.

"""
from __future__ import print_function

import argparse
import json
import pprint
import requests
import sys
import urllib
import pandas
import csv


# This client code can run on Python 2.x or 3.x.  Your imports can be
# simpler if you only need one of those.
try:
    # For Python 3.0 and later
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2 and urllib
    from urllib2 import HTTPError
    from urllib import quote
    from urllib import urlencode


# Yelp Fusion no longer uses OAuth as of December 7, 2017.
# You no longer need to provide Client ID to fetch Data
# It now uses private keys to authenticate requests (API Key)
# You can find it on
# https://www.yelp.com/developers/v3/manage_app
API_KEY = YOUR_API_KEY


# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.


# Defaults 
SEARCH_LIMIT = 50 #The maximum for a single request
OFFSET = 0 #Changing this will let us retrieve the next 50 results
SEARCH_TERM = "Coffee"

# Create an empty dataframe that we can add to. This is necessary because we have to make 20 
# separate requests that will each return 50 results in order to get the 1000 max total results
all_results = pandas.DataFrame()
all_totals = {}

locations= [
{'city':'Washington', 'state':'DC'}, 
{'city':'NewYork', 'state':'NY'},
{'city':'Boston', 'state':'MA'},
{'city':'Chicago', 'state':'IL'},
{'city':'Philadelphia', 'state':'PA'}, 
{'city':'Baltimore', 'state':'MD'},
{'city':'Seattle', 'state':'WA'},
{'city':'Atlanta', 'state':'GA'},
{'city':'LasVegas', 'state':'NV'},
{'city':'Portland', 'state':'OR'},
{'city':'Denver', 'state':'CO'},
{'city':'Detroit', 'state':'MI'},
{'city':'Minneapolis', 'state':'MN'},
{'city':'St.Louis', 'state':'MO'},
{'city':'Toronto', 'state':'ON'},
{'city':'Montreal', 'state':'QC'},
{'city':'Vancouver', 'state':'BC'},
{'city':'Edmonton', 'state':'AB'},
{'city':'Calgary', 'state':'AB'},
{'city':'SanFransisco', 'state':'CA'},
{'city':'LosAngeles', 'state':'CA'},
{'city':'Indianapolis', 'state':'IN'},
{'city':'Pittsburgh', 'state':'PA'},
{'city':'Columbus', 'state':'OH'},
{'city':'Cleveland', 'state':'OH'},
{'city':'Milwaukee', 'state':'WI'},
{'city':'Buffalo', 'state':'NY'},
{'city':'Sacramento', 'state':'CA'},
{'city':'Anchorage', 'state':'AK'},
{'city':'Dallas', 'state':'TX'},
{'city':'Fort Worth', 'state':'TX'},
{'city':'Houston', 'state':'TX'},
{'city':'Austin', 'state':'TX'},
{'city':'Omaha', 'state':'NE'},
{'city':'KansasCity', 'state':'MO'},
{'city':'Richmond', 'state':'VA'},
{'city':'London', 'state':'UK'},
{'city':'Manchester', 'state':'UK'},
{'city':'Birmingham', 'state':'UK'}
]

# Names of columns for Salesforce import format
cols = ["Lead Owner", "Company", "Venue Name", "First Name", "Last Name", "Email", "Title", "Street", "City", "Country", "State/Province", "Zip/Postal Code", "phone", "Venue Type", "Venue Notes", "Lead Status", "Opportunity Stage", "Objection Notes", "Status of Lead", "price", "rating", "review_count", "transactions", "url", "Yelp reviews url", "alias", "categories", "latitude", "longitude", "display_phone", "distance", "id", "image_url", "is_closed"]


def request(host, path, api_key, url_params=None):
    """Given your API_KEY, send a GET request to the API.

    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        API_KEY (str): Your API Key.
        url_params (dict): An optional set of query parameters in the request.

    Returns:
        dict: The JSON response from the request.

    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    #print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()


def search(api_key, term, location):
    """Query the Search API by a search term and location.

    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.

    Returns:
        dict: The JSON response from the request.
    """

    url_params = {
        'term': term,#.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT,
        'offset': OFFSET
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)


def get_business(api_key, business_id):
    """Query the Business API by a business ID.

    Args:
        business_id (str): The ID of the business to query.

    Returns:
        dict: The JSON response from the request.
    """
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, api_key)


def query_api(term, location):
    """Queries the API by the input values from the user.

    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """

    global all_results
    global cols
    response = search(API_KEY, term, location)

    businesses = response.get('businesses')
    total = response.get('total')
    all_totals[location] = total

    if not businesses:
        #print(u'No businesses for {0} in {1} found.'.format(term, location))
        return

    import sys
    import pandas as pd
    from pandas import DataFrame
    import json
    from pandas.io.json import json_normalize

    business_list_to_json = json.dumps(businesses)
    data_as_dict = json.loads(business_list_to_json)

    # Break location into separate columns for address, city, state, zipcode
    temp_df = json_normalize(data_as_dict)
    # Next we want to make the street address one column instead of 3
    def extract_address(address_list):
        return ', '.join(address_list[:-1])
    temp_df['location.address1'] = temp_df['location.display_address'].map(extract_address)
    temp_df = temp_df.drop(columns=['location.address2', 'location.address3', 'location.display_address'])
    
    # Extract the titles of the categories, concatenate, and separate with ;
    def extract_titles(categories_list):
        title_list = []
        for category in categories_list:
            title_list.append(category['title'])
        return ";".join(title_list)
    temp_df['categories'] = temp_df['categories'].map(extract_titles)

    # Generate the review search URLs
    def create_urls(url):
        return url + '&q="'+SEARCH_TERM+'"'
    temp_df['Yelp reviews url'] = temp_df['url'].map(create_urls)

    #Add the name columns
    temp_df["Venue Name"] = temp_df["name"]
    temp_df["Company"] = temp_df["name"]
    temp_df = temp_df.drop(columns=['name'])

    # rename columns to match salesforce template
    temp_df = temp_df.rename(columns={'location.address1':'Street', 'location.state': 'State/Province', 'location.zip_code': 'Zip/Postal Code', 'location.country':'Country', 'location.city':'City', 'coordinates.latitude':'latitude', 'coordinates.longitude':'longitude'})

    # add salesforce columns that don't come from the API
    old_cols = temp_df.columns.tolist()
    for c in cols:
        if c not in old_cols:
            temp_df[c] = None
    temp_df = temp_df[cols]

    all_results = all_results.append(temp_df)


def main():
    global OFFSET
    global all_results
    for loc in locations:
        all_results = pandas.DataFrame()
        print("Searching for "+SEARCH_TERM+" in "+loc['city']+", "+loc['state'])
        print("Progress: ", end='')
        for n in range(20):
            OFFSET = n*50
            print(' {0}%'.format(OFFSET//10+5), end='', flush=True)
            try:
                query_api(SEARCH_TERM, loc['city']+" , "+loc['state'])
            except HTTPError as error:
                sys.exit(
                    'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                        error.code,
                        error.url,
                        error.read(),
                    )
                )
        print("\n")
        all_results.to_csv(loc['city']+"_"+loc['state']+".csv")
    print("\n",all_totals)

if __name__ == '__main__':
    main()
