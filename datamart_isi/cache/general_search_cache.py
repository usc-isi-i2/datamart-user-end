from SPARQLWrapper import SPARQLWrapper, JSON, POST, URLENCODED
import memcache
import logging
import hashlib
import pickle
import pandas as pd
import datetime
import typing
import os
import json
from datamart_isi import config
from datamart_isi.utilities import connection
from d3m.container import DataFrame as d3m_DataFrame
from pandas.util import hash_pandas_object

MEMCAHCE_MAX_VALUE_SIZE = config.memcache_max_value_size


class GeneralSearchCache(object):
    def __init__(self, connection_url: str, memcache_max_value_size=MEMCAHCE_MAX_VALUE_SIZE):
        self._logger = logging.getLogger(__name__)
        self.memcache_server = connection.get_memcache_server_url(connection_url)
        self.general_search_server = connection.get_general_search_server_url(connection_url)
        self._logger.debug("Current memcache server url is: " + self.memcache_server)
        self._logger.debug("Current general search server url is: " + self.general_search_server)
        try:
            self.mc = memcache.Client([self.memcache_server], debug=True, server_max_value_length=memcache_max_value_size)
            self._logger.info("Start memcache connection to " + self.memcache_server + " success!")
        except Exception as e:
            self.mc = None
            self._logger.error("Start memcache connection to " + self.memcache_server + " failed!")
            self._logger.debug(e, exc_info=True)

        self.qm = SPARQLWrapper(self.general_search_server)
        self.qm.setReturnFormat(JSON)
        self.qm.setMethod(POST)
        self.qm.setRequestMethod(URLENCODED)
        # ensure folders exists
        for each_folder in ["", "wikifier_cache", "general_search_cache", "other_cache"]:
            storage_loc = os.path.join(config.cache_file_storage_base_loc, each_folder)
            if not os.path.exists(storage_loc):
                os.mkdir(storage_loc)

    def get_cache_results(self, hash_key) -> typing.Optional[bytes]:
        """
        Function used to check whether this query hash tag exist in memcache system or not
        :param hash_key: the hash key of the query
        :return: the cached query in pickled format if the hash key hit, otherwise None
        """
        if self.mc is not None:
            # check whether we have cache or not
            results = self.mc.get("augment_" + hash_key)
            if results:
                self._logger.info("Cache hit! will use this results.")
                try:
                    with open(results, "rb") as f:
                        results_loaded = pickle.load(f)
                    return results_loaded
                except Exception as e:
                    self._logger.warning("Hit results are broken! Need to rerun the query!")
                    self._logger.debug(e, exc_info=True)
            else:
                self._logger.info("Cache not hit.")
        else:
            self._logger.info("No memcache server connected, skip cache searching.")
        
        return None
            
    def add_to_memcache(self, supplied_dataframe, search_result_serialized, augment_results, hash_key) -> bool:
        try:
            self._logger.debug("Start pushing general augment result to " + self.memcache_server)
            # add query results
            if type(supplied_dataframe) is d3m_DataFrame or type(supplied_dataframe) is pd.DataFrame:
                hash_supplied_dataframe = hash_pandas_object(supplied_dataframe).sum()
            else:
                raise ValueError("Unsupport type of supplied_data result as " + str(type(supplied_dataframe)) + "!")

            # add supplied data for further updating if needed
            try:
                search_result_json = json.loads(search_result_serialized)
                if "wikifier_choice" in search_result_json:
                    storage_loc = os.path.join(config.cache_file_storage_base_loc, "wikifier_cache")
                else:
                    storage_loc = os.path.join(config.cache_file_storage_base_loc, "general_search_cache")
            except:
                storage_loc = os.path.join(config.cache_file_storage_base_loc, "other_cache")

            path_to_supplied_dataframe = os.path.join(storage_loc, str(hash_supplied_dataframe) + ".pkl")
            path_to_augment_results = os.path.join(storage_loc, hash_key + ".pkl")

            with open(path_to_augment_results, "wb") as f:
                pickle.dump(augment_results, f)

            response_code1 = self.mc.set("augment_" + hash_key, path_to_augment_results)
            if not response_code1:
                self._logger.warning("Pushing wikidata search result failed! Maybe the size too big?")

            # add timestamp to let the system know when to update
            response_code2 = self.mc.set("timestamp_" + hash_key, str(datetime.datetime.now().timestamp()))
            if not response_code2:
                self._logger.warning("Pushing timestamp failed! What happened???")

            with open(path_to_supplied_dataframe, "wb") as f:
                pickle.dump(supplied_dataframe, f)
            response_code3 = self.mc.set("supplied_data" + hash_key, path_to_supplied_dataframe)
            if not response_code3:
                self._logger.warning("Pushing supplied data failed! Maybe the size too big?")

            # add search result
            response_code4 = self.mc.set("search_result" + hash_key, search_result_serialized)
            if not response_code4:
                self._logger.warning("Pushing search result failed! Maybe the size too big?")

            # only return True if all success
            if response_code1 and response_code2 and response_code3 and response_code4:
                self._logger.info("Pushing search result success!")
                return True
            else:
                return False
        except Exception as e:
            self._logger.error("Pushing results of general search hash key " + hash_key + " failed!")
            self._logger.debug(e, exc_info=True)
            return False

    def get_hash_key(self, supplied_dataframe: d3m_DataFrame, search_result_serialized: str) -> str:
        """
        get the hash key for the this general search result
        :param supplied_dataframe: supplied dataframe
        :param search_result_serialized: serialized search result
        :return: a str represent the hash key
        """
        hash_supplied_data = hash_pandas_object(supplied_dataframe).sum()
        hash_generator = hashlib.md5()
        hash_generator.update(search_result_serialized.encode('utf-8'))
        hash_search_result = hash_generator.hexdigest()
        hash_key = str(hash_supplied_data) + str(hash_search_result)
        self._logger.debug("Current search's hash tag is " + hash_key)
        return hash_key
