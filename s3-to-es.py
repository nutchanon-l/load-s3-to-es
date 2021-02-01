import csv
import json
import boto3
import traceback
import sys
import codecs
import uuid
from elasticsearch import Elasticsearch
from elasticsearch import helpers

config_file = "config.json"
metadata_file = "metadata.json"
bulk_batch_size = 1000

class S3Control:
    def __init__(self, config: str):
        s3_config = read_conf(config)['s3']
        self.bucket = s3_config['bucket']
        self.prefix = s3_config['prefix'] if s3_config['prefix'][-1] == '/' else s3_config['prefix'] + '/'
        self.s3_client = boto3.client('s3')
        
    def list_data_objects(self) -> list:
        response = self.s3_client.list_objects(Bucket=self.bucket, Prefix=self.prefix)
        objects = []
        for key in response['Contents']:
            objects.append(key['Key'])
        return objects
        
    def read_csv_file(self, filename: str) -> csv.DictReader:
        csv_obj = self.s3_client.get_object(Bucket=self.bucket, Key=filename)
        body = csv_obj['Body']
        csv_string = csv.DictReader(codecs.getreader("utf-8")(body))
        return csv_string

class ESControl:
    def __init__(self, config: str):
        es_config = read_conf(config)['elasticsearch']
        self.url = es_config['url']
        self.port = es_config['port']
        self.user = es_config['username']
        self.pwd = es_config['password']
        self.conn = None

    def connect(self):
        try:
            es = Elasticsearch(
                self.url,
                http_auth=(self.user, self.pwd),
                scheme="https",
                port=self.port,
            )
            self.conn = es
        except Exception as e:
            print("Unknown error exception")
            traceback.print_exc()
            sys.exit(2)
        
class Metadata:
    def __init__(self, metadata: str):
        meta_conf = read_conf(metadata)
        self.index = meta_conf['index']['es_index_name'] if meta_conf['index']['es_index_as_filename'] is False else None
        self.headers = meta_conf['data']['headers']
        self.id_field = meta_conf['data']['id_field'] if meta_conf['data']['id_field'] != "" else None
        
    def set_index_name(self, name: str):
        self.index = name

def read_conf(conf_file: str) -> dict:
    with open(conf_file, "r") as f:
        conf = json.loads(f.read())
    f.close()
    return conf

def bulk_write_to_es(es_conn, es_index: str, es_headers: list, es_id_field: str, es_body: csv.DictReader, batch_size: int):
    ## batch control
    row_count = 0
    
    ## bulk actions
    actions = []
    
    ## info
    print("INFO: Starting to bulk write into index: {}".format(es_index))
    print("INFO: Bulk size is: {}".format(batch_size))
    
    ## build document
    for data_row in es_body:
        # define document id
        doc_id = str(uuid.uuid1()) if es_id_field is None else data_row[es_id_field]
        
        # define _source of document
        source_dict = {}
        for es_header in es_headers:
            source_dict[es_header] = data_row[es_header]
        
        # define a document
        es_doc = {
            "_index": es_index,
            "_type": "_doc",
            "_id": doc_id,
            "_source": source_dict
        }
        actions.append(es_doc)
        
        # increase batch size
        row_count += 1
        
        # batch process
        if row_count % bulk_batch_size == 0:
            bulk_es(es_conn, actions)
            actions = []
            
    # final batch size
    bulk_es(es_conn, actions)
    print("INFO: Bulk write succeed: {} docuemnts".format(row_count))
    
def bulk_es(es_conn, data: dict):
    # bulk process
    try:
        print("Number of processing documents: {}".format(len(data)))
        res = helpers.bulk(es_conn, data)
        print("Bulk write succeed: {} documents".format(res[0]))
    except Exception as e:
        print("Exception: {}".format(str(e)))
        traceback.print_exc()
        sys.exit(2)

def main():
    # define configuration
    s3_obj = S3Control(config_file)
    es_obj = ESControl(config_file)
    md_obj = Metadata(metadata_file)
    
    # connect to Elasticsearch cluster
    es_obj.connect()
    
    # read data from csv
    data_objects = s3_obj.list_data_objects()
    
    # check index metadata information
    if md_obj.index is None:
        file_as_index = True
    else:
        file_as_index = False
    
    # process each file
    for data_obj in data_objects:
        # check index name condition
        if file_as_index is True:
            index_name = data_obj.split('/')[-1].split('.')[0]
            md_obj.set_index_name(index_name)
        
        # read data from s3
        s3_data_body = s3_obj.read_csv_file(data_obj)
        
        # write data to elasticsearch
        bulk_write_to_es(es_obj.conn, md_obj.index, md_obj.headers, md_obj.id_field, s3_data_body, bulk_batch_size)
    
if __name__ == "__main__":
    main()