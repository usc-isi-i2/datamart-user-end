import os
import socket
host_name = socket.gethostname()

if host_name == "dsbox02":
    home_dir = "/data00/dsbox/datamart"
else:
    home_dir = os.getenv("HOME")

default_datamart_url = "dsbox02.isi.edu"

# wikidata_server = "http://dsbox02.isi.edu:8888/bigdata/namespace/wdq/sparql"
# general_search_server = "http://dsbox02.isi.edu:9002/blazegraph/namespace/datamart3/sparql"
# wikifier_server = "dsbox02.isi.edu:4444/get_identifiers"
# memcache_server = "dsbox02.isi.edu:11211"

cache_file_storage_base_loc = os.path.join(home_dir, "memcache_storage")

wikidata_server_suffix = ":8888/bigdata/namespace/wdq/sparql"
memcache_server_suffix = ":11211"
wikifier_server_suffix = ":9000/get_identifiers"
general_search_server_suffix = ":9002/blazegraph/namespace/datamart3/sparql"
general_search_test_server_suffix = ":9002/blazegraph/namespace/datamart4/sparql"
rest_api_suffix = ":9000"
redis_server_port = "6379"


# current memcache server's max size is 100MB
memcache_max_value_size = 1024*1024*100
# current cache expiration time is 24 hours
cache_expire_time = 3600*24

# following are datamart detail configs, usually these should not be changed
augmented_column_semantic_type = "https://metadata.datadrivendiscovery.org/types/Datamart_augmented_column"
augmented_resource_id = "learningData"
d3m_container_version = "https://metadata.datadrivendiscovery.org/schemas/v0/container.json"
p_nodes_ignore_list = {"P1549"}
q_node_semantic_type = "http://wikidata.org/qnode"
# this is used to add some speical part in query to constrain p nodes search results
special_request_for_p_nodes = {"P1813": "FILTER(strlen(str(?P1813)) = 2)"}
time_column_mark = "%^&*SPECIAL_TIME_TYPE%^&*"
wikifier_column_mark = "%^$#@wikifier@%^$#"
max_entities_length = 10000

need_wikifier_column_type_list = {"https://metadata.datadrivendiscovery.org/types/CategoricalData",
                                  "http://schema.org/Text"
                                  }

skip_wikifier_column_type_list = {"https://metadata.datadrivendiscovery.org/types/PrimaryKey",
                                  "https://metadata.datadrivendiscovery.org/types/SuggestedTarget",
                                  "https://metadata.datadrivendiscovery.org/types/TrueTarget",
                                  "https://metadata.datadrivendiscovery.org/types/Target",
                                  "https://metadata.datadrivendiscovery.org/types/SuggestedTarget",
                                  "http://schema.org/Float",
                                  "http://schema.org/Integer"}
default_temp_path = "/tmp"

# elastic search to fetch FB embeddings
wikidata_uri_template = '<http://www.wikidata.org/entity/{}>'
em_es_url = "http://sitaware.isi.edu:9200"
em_es_index = "wiki_fb_embeddings_1"
em_es_type = "vectors"

# new wikifier server
# new_wikifier_server = "http://dsbox02.isi.edu:8396/wikify"
new_wikifier_server = "http://minds03.isi.edu:8396/wikify"


max_longitude_val = 180
min_longitude_val = -180
max_latitude_val = 90
min_latitude_val = -90
