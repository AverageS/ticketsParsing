from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


MAPPING = {
    "mappings": {
        "_default_": {
            "dynamic_templates": [{
                "string_fields": {
                    "mapping": {
                        "index": "analyzed",
                        "omit_norms": True,
                        "type": "string",
                        "fields": {
                            "raw": {
                                "ignore_above": 1024,
                                "index": "not_analyzed",
                                "type": "string"
                            }
                        }
                    },
                    "match_mapping_type": "string",
                    "match": "*"
                }
            }
            ],
            "properties": {
                "doc_creation_date": { "type": "date"},
                "added_time":  { "type": "date"},
                "ip_host": {"type" : "ip"},
                "ip_dest": {"type" : "ip"},
                "port_dest": {"type" : "string"},
            }
        }
    }
}
def create_mapping(es_instance, index_name):
    try:
        es.indices.delete(index=index_name)
    except:
        pass
    es_instance.indices.create(index=index_name, ignore=400, body=MAPPING)

if __name__ == "__main__":
    es.indices.delete(index='tickets')
    create_mapping(es, 'tickets')