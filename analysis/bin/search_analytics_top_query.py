#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#
# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Example for using the Google Search Analytics API (part of Search Console API).

A basic python command-line example that uses the searchAnalytics.query method
of the Google Search Console API. This example demonstrates how to query Google
search results data for your property. Learn more at
https://developers.google.com/webmaster-tools/

To use:
1) Install the Google Python client library, as shown at https://developers.google.com/webmaster-tools/v3/libraries.
2) Sign up for a new project in the Google APIs console at https://code.google.com/apis/console.
3) Register the project to use OAuth2.0 for installed applications.
4) Copy your client ID, client secret, and redirect URL into the client_secrets.json file included in this package.
5) Run the app in the command-line as shown below.

 usage:

  $ python search_analytics_api_sample.py 'https://www.example.com/' '2015-05-01' '2015-05-30' --noauth_local_webserver

"""

import argparse
import sys
from googleapiclient import sample_tools
from pymongo import MongoClient
sys.path.append("..")
from settings import *

# Declare command-line flags.
argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument('property_uri', type=str,
                       help=('Site or app URI to query data for (including '
                             'trailing slash).'))
argparser.add_argument('start_date', type=str,
                       help=('Start date of the requested date range in '
                             'YYYY-MM-DD format.'))
argparser.add_argument('end_date', type=str,
                       help=('End date of the requested date range in '
                             'YYYY-MM-DD format.'))

# Declare mongo database variables
client = MongoClient(mongo_hostname, monggo_port)
db_edumarket = client[mongo_dbname_edumarket]


def main(argv):
	service, flags = sample_tools.init(
		argv, 'webmasters', 'v3', __doc__, __file__, parents=[argparser],
		scope='https://www.googleapis.com/auth/webmasters.readonly')

	# Get top 60 queries for the date range, sorted by click count, descending.
	request = {
		'startDate': flags.start_date,
      		'endDate': flags.end_date,
      		'dimensions': ['query'],
      		'rowLimit': 5000
  	}
	response = execute_request(service, flags.property_uri, request)
	rows = response['rows']
	for row in rows:
		keys = ''
  		# Keys are returned only if one or more dimensions are requested.
		if 'keys' in row :
			keys = u','.join(row['keys'])
		if (db_edumarket.google_top_query.find({'keyword':keys}).count()  == 0)  :
			db_edumarket.google_top_query.insert_one({'keyword':keys, 'clicks':row['clicks'], 'impressions':row['impressions'], 'ctr':row['ctr'], 'position':row['position'], 'isdelete':'N'})
		else :
			db_edumarket.google_top_query.update_one(
				{'keyword':keys},
				{'$set' : {'keyword':keys, 'clicks':row['clicks'], 'impressions':row['impressions'], 'ctr':row['ctr'], 'position':row['position']}},
				upsert=False)
	client.close()


def execute_request(service, property_uri, request):
	"""Executes a searchAnalytics.query request.

  	Args:
    	service: The webmasters service to use when executing the query.
    	property_uri: The site or app URI to request data for.
    	request: The request to be executed.

  	Returns:
    	An array of response rows.
  	"""
	return service.searchanalytics().query(siteUrl=property_uri, body=request).execute()


if __name__ == '__main__' :
	main(sys.argv)
	print('search_analytics_to_query finished.')