from elasticsearch import Elasticsearch
import json
import csv

data_for_es = []
columns = []
with open('data/addresses.csv') as csv_file:
    dict_reader = csv.DictReader(csv_file, delimiter='|')
    line_count = 0
    for col in dict_reader.fieldnames:
        columns.append(col)
    for row in dict_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1
        line_count += 1
        data_for_es.append(row)
    print(f"Processed {line_count} lines.")


# configure elasticsearch

es = Elasticsearch()

request_body = {
        "settings" : {
            "number_of_shards": 5,
            "number_of_replicas": 1
        },

        'mappings': {
            'examplecase': {
                'properties': {
                    'address': {'index': 'not_analyzed', 'type': 'string'},
                    'date_of_birth': {'index': 'not_analyzed', 'format': 'dateOptionalTime', 'type': 'date'},
                    'some_PK': {'index': 'not_analyzed', 'type': 'string'},
                    'fave_colour': {'index': 'analyzed', 'type': 'string'},
                    'email_domain': {'index': 'not_analyzed', 'type': 'string'},
                    "logdate": {"type":"date","format":"yyyy-MM-dd"}
                }}}
    }


print("creating 'example_index' index...")
es.indices.create(index = 'example_index', ignore=400, body = request_body)


# Prepare data
bulk_data = []

for index, row in enumerate(data_for_es):
    data_dict = {}
    for i in range(len(row)):
        data_dict[columns[i]] = row[columns[i]]
    op_dict = {
        "index": {
            "_index": "example_index",
            "_type": "examplecase",
            "_id": data_dict['some_PK']
        }
    }
    bulk_data.append(op_dict)
    bulk_data.append(data_dict)

res = es.bulk(index = "example_index", body = bulk_data)

# check data is in there, and structure in there
es.search(body={"query": {"match_all": {}}}, index = "example_index")
es.indices.get_mapping(index = "example_index")