import requests
import json
from datetime import datetime
from boto3 import resource
from boto3.dynamodb.conditions import Key
from decimal import *
from elasticsearch import Elasticsearch
from io import BytesIO
import csv

class YelpSearch(object):
    def __init__(self):
        self.MY_API_KEY = "nwF7OCB76Z2tdeqOXjl94qLObvgQ5rE0xOsr83IxapRICtmttXrwndM44pxcwQkxV0BSSeKywMhEw1_WVGDvOrMSfMhlIQyOjtE_NjUyMV8QdpsFTCWCzCIV_MUqZXYx"

    def search(self, term, location, offset):
        headers = {'Authorization': 'Bearer %s' % self.MY_API_KEY}
        url='https://api.yelp.com/v3/businesses/search'

        params = {'term': term, 'location':location, 'offset': offset}
        req=requests.get(url, params=params, headers=headers)

        if req.status_code != 200:
            print('The status code is {}, please try again.'.format(req.status_code))
            return []
        result = json.loads(req.text)
        return (result['businesses'])

    def get_all(self, term, location='Manhattan'):
        headers = {'Authorization': 'Bearer %s' % self.MY_API_KEY}
        url='https://api.yelp.com/v3/businesses/search'
        params = {'term': term, 'location':location}
        req=requests.get(url, params=params, headers=headers)
        restaurant_cnt = json.loads(req.text).get('total')

        results = []
        for offset in range(0, restaurant_cnt, 20):
            results = results + self.search(term, location, offset)
        return results

class DynamoProcessor(object):
    def __init__(self):
        self.dynamodb_resource = resource('dynamodb', region_name='us-east-1', 
        aws_access_key_id='REDACTED',
        aws_secret_access_key= 'REDACTED' )

    def add_item(self, col_dict, cusine_type, table_name='yelp-restaurants'):
        table = self.dynamodb_resource.Table(table_name)
        col_dict['insertedAtTimestamp'] = datetime.now().strftime( '%Y-%m-%dT%H::%M::%S.%f')
        col_dict['cusine_types'] = [cusine_type]
        drop_keys = ['location', 'categories']
        new_dict = {}
        # check if the restaurant already exist
        exist_restaurant = table.get_item(Key={'id':col_dict['id']})
        if 'Item' in exist_restaurant:
            previous_cusines = exist_restaurant['Item'].get('cusine_types')
            if cusine_type not in previous_cusines:
                response = table.update_item(
                    Key={'id': col_dict['id']},
                    UpdateExpression="SET cusine_types = list_append(cusine_types, :i)",
                    ExpressionAttributeValues={
                        ':i': [cusine_type],
                    }
                )
                return response
            return None

        for key in col_dict:
            if not col_dict[key]:
                drop_keys.append(key)
            if type(col_dict[key]) is float:
                col_dict[key] = Decimal(str(col_dict[key]))
            if key == 'coordinates':
                col_dict[key]['latitude'] = Decimal(str(col_dict[key]['latitude']))
                col_dict[key]['longitude'] = Decimal(str(col_dict[key]['longitude']))
            if key == 'location':
                for key2 in col_dict[key]:
                    if col_dict[key][key2]:
                        new_dict[key2] = col_dict[key][key2]
        for key in drop_keys:
            col_dict.pop(key)
        col_dict.update(new_dict)
        response = table.put_item(Item=col_dict)
        return response

    def process_dict(col_dict):
        pass


    def add_items(self, data, cusine_type, table_name='yelp-restaurants'):
        for col_dict in data:
            self.add_item(col_dict, cusine_type, table_name)


class ElasticSearchUploader(object):
    def __init__(self):
        super(ElasticSearchUploader, self).__init__()
        self.host = 'https://search-resturant-search-2uf2anjqb5myuf3grjoqdm7yzu.us-east-1.es.amazonaws.com:443'

        self.elasticSearch = Elasticsearch(
            [self.host],
            timeout=1000,
            basic_auth=('elastic', '<>'),
            verify_certs=True)
            
    def add_items(self, data, cusine_type):
        for col_dict in data:
            response = self.add_item(col_dict, cusine_type)

    def add_item(self, col_dict, cusine_type, table_name='yelp-restaurants'):
        index_info = {
            'id': col_dict['id'],
            'cuisine': cusine_type
        }



class BulkUploadTextWriter(object):
    def __init__(self):
        super(BulkUploadTextWriter, self).__init__()
    
    def add_item_from_dict(self, col_dict, cusine_type):
        # extract id and cusine

        # print('col_dict:', col_dict)
        # print('-----------')
        line_1 = "{\"index\": {\"_index\": \"Restaurant\", " + "\"_id\": " + col_dict['id'] + "} }"
        line_2 = "{\"restaurant_id\":" + col_dict['id'] + ", \"cusine_type\": \"" + cusine_type + "\"}"
        
        with open('restaurants.txt', 'a') as f:
            f.write(line_1 + '\n')
            f.write(line_2 + '\n')
            f.close()

    def add_items(self, data, cusine_type):
        for col_dict in data:
            self.add_item(col_dict, cusine_type)

    def add_item_from_csv(self):
        # open the CSV file

        counter = 1

        with open('businesses.csv') as file_obj: 
      
            # Create reader object by passing the file  
            # object to reader method 
            reader_obj = csv.reader(file_obj) 
            
            # Iterate over each row in the csv  
            # file using reader object 
            for row in reader_obj: 
                id = row[0]
                cusine_type = row[1]

                line_1 = "{\"index\": {\"_index\": \"restaurants\", " + "\"_id\": " + str(counter) + "} }"
                line_2 = "{\"restaurant_id\": \"" + id + "\" , \"cusine_type\": \"" + cusine_type + "\"}"

                counter = counter + 1
        
                with open('restaurants_from_csv.txt', 'a') as f:
                    f.write(line_1 + '\n')
                    f.write(line_2 + '\n')
                    f.close()

    

class CSVCreatorForRestaurants(object):
    def __init__(self):
        super(CSVCreatorForRestaurants, self).__init__()
        self.processed_restaurants = set() # set to keep track of processed restaurants

    def add_items(self, data, cusine_type):
        for col_dict in data:
            self.add_item(col_dict, cusine_type)

    def add_item(self, col_dict, cusine_type):
        
        # check if business_id already processed
        if col_dict['id'] in self.processed_restaurants:
            return

        with open('businesses.csv', 'a', newline='') as file:
            writer = csv.writer(file)
     
            writer.writerow([col_dict['id'], cusine_type])
            # add to the list
            self.processed_restaurants.add(col_dict['id'])
    



if __name__ == '__main__':
    yelpSearch = YelpSearch()
    print('after init YelpSearch')
    all_types = ['Vegan', 'Indian', 'Japanese', 'Taiwanese', 'Italian', 'Mexican']
    for t in all_types:
        print('processing t:', t)
        results = yelpSearch.get_all('{} restaurant'.format(t))
        # print('results:', results)
        db = DynamoProcessor()
        db.add_items(results, t)
        es = ElasticSearchUploader()
        es.add_items(results, t)
        dict = {'id': 'id_here', 'cusine_type': t}
        bulk_uploader = BulkUploadTextWriter()
        bulk_uploader.add_items(results, t)

        csv_creator = CSVCreatorForRestaurants()
        csv_creator.add_items(results, t)

        bulk_uploader = BulkUploadTextWriter()
        bulk_uploader.add_item_from_csv()
