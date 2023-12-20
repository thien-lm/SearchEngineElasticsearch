from elasticsearch import Elasticsearch, helpers
import csv

# Create the elasticsearch client.
es = Elasticsearch(hosts=["http://localhost:9200"])

index_name = "question_index"

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
      "type": "text"
    },
    "comment_text_v2": {
      "type": "text"
    }
  }
})

with open("normalize_input.csv", "r") as f:
  reader = csv.DictReader(f)
  id_counter = 1  # Initialize a counter for the document ID

  for row in reader:
    document = {
      "title": row["Title"],
      "comment_text": row["comment_text"],
      "comment_text_v2": row["comment_text_v2"],
      "tags": row["Tags"]
    }

    es.index(index=index_name, id=id_counter, body=document)  # Specify the document ID when indexing
    id_counter += 1  # Increment the counter
