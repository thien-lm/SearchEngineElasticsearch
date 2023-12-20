from elasticsearch import Elasticsearch, helpers
import csv

# Create the elasticsearch client.

es = Elasticsearch(hosts=["http://localhost:9200"])

index_name = "my_index"

es.indices.create(index=index_name)

document_type = "my_type"

es.indices.put_mapping(index=index_name, body={
  "properties": {
    "title": {
      "type": "text"
    },
    "comment_text": {
      "type": "text"
    },
    "tags": {
      # "type": "nested",
      # "properties": {
      #   "name": {
      #     "type": "keyword"
      #   }
      # }
      "type": "text"
    }
  }
})

import csv

with open("data.csv", "r") as f:
  reader = csv.DictReader(f)

  for row in reader:
    document = {
      "title": row["Title"],
      "comment_text": row["comment_text"],
      "tags": row["Tags"]
    }

    # for tag in row["Tags"].split(","):
    #   document["tags"].append({"name": tag})

    es.index(index=index_name, body=document)

