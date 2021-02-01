# Load CSV data from Amazon S3 to Elasticsearch
This python script is used to load CSV files from Amazon S3 to Elasticsearch, weather self-managed or Amazon ES.

The script is tested on AWS Cloud9 and it is recommended to use Cloud9 for prevent data movement issue.

## Prerequisites
* Python 3.7+
* Required packages
```
$ pip install -r requirements.txt
```
* Permission to access S3 bucket (either IAM policy or bucket policy)

## How To Use
1. Setup config.json
```
{
    "s3": {
        "bucket": "",   // S3 bucket name
        "prefix": ""    // S3 prefix
    },
    "elasticsearch": {
        "url": [""],    // Elasticsearch URL
        "port": 443,    // Elasticsearch port
        "username": "", // Elasticsearch Username
        "password": ""  // Elasticsearch Password
    }
}
```
2. Setup metadata.json
```
{
    "index": {
        "es_index_as_filename": false,  // if the value is false, filename will be index name.
        "es_index_name": "" // explicit index name, you have to set "es_index_as_filename" to false when using this field.
    },
    "data": {
        "headers": [], // field names, must be match with headers of csv file.
        "id_field": "" // field name that contain document id, if the value is "", UUID will be used.
    }
}
```
3. **Ensure that the first row of CSV data is field headers**

4. (Optional) Configure Elasticsearch Template for explicit define index mappings

5. (Optional) Modify "bulk_batch_size" for tune bulk performance
 
## How To Run
```
$ python s3-to-es.py
```

## Elasticsearch Template
**Kibana Dev Tool: Upload index template**
```
POST _template/mytemplate
{Value of elasticsearch/es-template.json}
```