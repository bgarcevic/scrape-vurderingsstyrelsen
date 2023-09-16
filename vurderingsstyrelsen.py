from elasticsearch import Elasticsearch
import json

es = Elasticsearch(['https://api-fs.vurderingsportalen.dk'])

index_name = 'preliminaryproperties'
page_size = 1000

def search(index, page_size, search_after=None):
    body = {
        "size": page_size,
        "query": {
            "match_all": {}
        },
        "sort": [
            { "_id" : "asc" },
            "_score"
        ]
    }
    
    if search_after:
        body['search_after'] = search_after
    
    return es.search(index=index, **body)

document_count = 0

with open('output.jsonl', 'w') as f:
    # Initial search
    print("Starting data retrieval from Elasticsearch...")
    response = search(index_name, page_size)

    # Paginate through all results
    while len(response['hits']['hits']) > 0:
        fetched_count = len(response['hits']['hits'])
        document_count += fetched_count
        print(f"Fetched {fetched_count} documents, total so far: {document_count}")

        for hit in response['hits']['hits']:
            # Convert the document to JSON format and write to the file
            f.write(json.dumps(hit['_source']))
            f.write('\n')

        # Get sort values of the last document for next page request
        last_hit_sort = response['hits']['hits'][-1]['sort']
        response = search(index_name, page_size, search_after=last_hit_sort)

print("Data retrieval complete!")
print(f"Total documents saved: {document_count}")
print("Data has been saved to output.jsonl")