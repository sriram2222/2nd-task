
import numpy as np

from elasticsearch import Elasticsearch, NotFoundError
import pandas as pd


# Replace with your actual username and password
es = Elasticsearch(
    hosts=["http://localhost:9200"],
    basic_auth=("elastic", "Xxc0c10HfeSBGlp=Ms4L")
)


def createCollection(p_collection_name):
    # Convert the collection name to lowercase
    p_collection_name = p_collection_name.lower()
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Collection {p_collection_name} created.")
    else:
        print(f"Collection {p_collection_name} already exists.")


def indexData(p_collection_name, department):
    df = pd.read_csv('D:\\projectss\\elastic\\employee_data.csv', encoding='ISO-8859-1')  # Load your data from a CSV
    for i, row in df.iterrows():
        doc = row.to_dict()  # Convert row to a dictionary
        # Replace NaN values with None or an appropriate default value
        doc = {k: (v if pd.notna(v) else None) for k, v in doc.items()}
        es.index(index=p_collection_name, id=i, body=doc)


def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    res = es.search(index=p_collection_name, body=query)
    for hit in res['hits']['hits']:
        print(hit['_source'])

def getEmpCount(p_collection_name):
    # Ensure collection name is in lowercase
    p_collection_name = p_collection_name.lower()
    try:
        res = es.count(index=p_collection_name)
        print(f"Total employees in {p_collection_name}: {res['count']}")
    except elasticsearch.NotFoundError:
        print(f"Index {p_collection_name} not found.")


from elasticsearch import Elasticsearch, NotFoundError

# Function to delete an employee by ID
def delEmpById(p_collection_name, p_employee_id):
    try:
        # Check if the document exists
        res = es.get(index=p_collection_name, id=p_employee_id)
        if res['found']:
            es.delete(index=p_collection_name, id=p_employee_id)
            print(f"Employee with ID {p_employee_id} deleted successfully.")
        else:
            print(f"Employee with ID {p_employee_id} not found.")
    except NotFoundError:
        print(f"Employee with ID {p_employee_id} not found.")



def getDepFacet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {
            "department_facet": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    res = es.search(index=p_collection_name, body=query)
    for bucket in res['aggregations']['department_facet']['buckets']:
        print(f"{bucket['key']}: {bucket['doc_count']}")

# Executing functions
# Call the function with lowercase collection names
v_nameCollection = 'hash_yourname'.lower()  # Replace 'yourname' with your actual name
v_phoneCollection = 'hash_1234'.lower()     # Replace '1234' with the last 4 digits of your phone number

# This line should work now since the index name is lowercase
getEmpCount(v_nameCollection)

createCollection(v_nameCollection)
createCollection(v_phoneCollection)

getEmpCount(v_nameCollection)
indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')

delEmpById(v_nameCollection, 'E02003')
getEmpCount(v_nameCollection)

searchByColumn(v_nameCollection, 'Department', 'IT')
searchByColumn(v_nameCollection, 'Gender', 'Male')
searchByColumn(v_phoneCollection, 'Department', 'IT')

getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)
