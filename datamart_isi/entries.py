import typing
import pandas as pd
import copy
import os
import random
import collections
import typing
import logging
import json
import string
import time
from ast import literal_eval
from itertools import combinations

from d3m import container
from d3m import utils
from d3m.container import DataFrame as d3m_DataFrame
from d3m.container import Dataset as d3m_Dataset
from d3m.base import utils as d3m_utils
from d3m.metadata.base import DataMetadata, ALL_ELEMENTS

from datamart import TabularVariable, ColumnRelationship, AugmentSpec
from datamart_isi import config
from datamart_isi.augment import Augment
from datamart_isi.joiners.rltk_joiner import RLTKJoinerGeneral
from datamart_isi.joiners.rltk_joiner import RLTKJoinerWikidata
from datamart_isi.utilities.utils import Utils
from datamart_isi.utilities.timeout import timeout_call
from datamart_isi.utilities import d3m_wikifier
from datamart_isi.utilities.d3m_metadata import MetadataGenerator
from datamart_isi.utilities.download_manager import DownloadManager
from datamart_isi.cache.wikidata_cache import QueryCache
from datamart_isi.cache.general_search_cache import GeneralSearchCache
from datamart_isi.cache.metadata_cache import MetadataCache
# from datamart_isi.joiners.join_result import JoinResult
# from datamart_isi.joiners.joiner_base import JoinerType


__all__ = ('DatamartQueryCursor', 'Datamart', 'DatasetColumn', 'DatamartSearchResult', 'AugmentSpec',
           'TabularJoinSpec', 'TemporalGranularity', 'ColumnRelationship', 'DatamartQuery',
           'VariableConstraint',  'TabularVariable', 'VariableConstraint')

Q_NODE_SEMANTIC_TYPE = config.q_node_semantic_type
AUGMENTED_COLUMN_SEMANTIC_TYPE = config.augmented_column_semantic_type
MAX_ENTITIES_LENGTH = config.max_entities_length
P_NODE_IGNORE_LIST = config.p_nodes_ignore_list
SPECIAL_REQUEST_FOR_P_NODE = config.special_request_for_p_nodes
AUGMENT_RESOURCE_ID = config.augmented_resource_id
WIKIDATA_QUERY_SERVER_SUFFIX = config.wikidata_server_suffix
MEMCACHE_SERVER_SUFFIX = config.memcache_server_suffix
DEFAULT_DATAMART_URL = config.default_datamart_url
TIME_COLUMN_MARK = config.time_column_mark


class DatamartQueryCursor(object):
    """
    Cursor to iterate through Datamarts search results.
    """

    def __init__(self, augmenter, search_query, supplied_data, need_run_wikifier=None, connection_url=None):
        """
        :param augmenter: The manager used to parse query and search on datamart general part(blaze graph),
                          because it search quick and need instance update, we should not cache this part
        :param search_query: query generated from Datamart class
        :param supplied_data: supplied data for search
        :param need_run_wikifier: an optional parameter, can help to control whether need to run wikifier to get
                                  wikidata-related parts, it can help to improve the speed when processing large data
        :param connection_url: control paramter for the connection url
        """
        self._logger = logging.getLogger(__name__)
        if connection_url:
            self._logger.info("Using user-defined connection url as " + connection_url)
            self.connection_url = connection_url
        else:
            # TODO: currently temporary add also to get nyu's datamart url here, should set to use isi's in the future
            connection_url = os.getenv('DATAMART_URL_NYU', DEFAULT_DATAMART_URL)
            self.connection_url = connection_url
        self._logger.debug("Current datamart connection url is: " + self.connection_url)
        self.augmenter = augmenter
        self.search_query = search_query
        self.supplied_data = supplied_data
        self.current_searching_query_index = 0
        self.remained_part = None
        self.wikidata_cache_manager = QueryCache(connection_url=self.connection_url)
        if need_run_wikifier is None:
            self.need_run_wikifier = self._check_need_wikifier_or_not()
        else:
            self.need_run_wikifier = need_run_wikifier
        self.q_nodes_columns = list()

    def get_next_page(self, *, limit: typing.Optional[int] = 20, timeout: int = None) \
            -> typing.Optional[typing.Sequence['DatamartSearchResult']]:
        """
        Return the next page of results. The call will block until the results are ready.

        Note that the results are not ordered; the first page of results can be returned first simply because it was
        found faster, but the next page might contain better results. The caller should make sure to check
        `DatamartSearchResult.score()`.

        Parameters
        ----------
        limit : int or None
            Maximum number of search results to return. None means no limit.
        timeout : int
            Maximum number of seconds before returning results. An empty list might be returned if it is reached.

        Returns
        -------
        Sequence[DatamartSearchResult] or None
            A list of `DatamartSearchResult's, or None if there are no more results.
        """
        if timeout is None:
            timeout = 1800
        self._logger.info("Set time limit to be " + str(timeout) + " seconds.")

        # if need to run wikifier, run it before any search
        if self.current_searching_query_index == 0 and self.need_run_wikifier:
            self.supplied_data = self.run_wikifier(self.supplied_data)

        # if already remained enough part
        current_result = self.remained_part or []
        if len(current_result) > limit:
            self.remained_part = current_result[limit:]
            current_result = current_result[:limit]
            return current_result

        # start searching
        while self.current_searching_query_index < len(self.search_query) and len(current_result) < limit:
            time_start = time.time()
            self._logger.debug("Start searching on query No." + str(self.current_searching_query_index))

            if self.search_query[self.current_searching_query_index].search_type == "wikidata":
                # TODO: now wikifier can only automatically search for all possible columns and do exact match
                search_res = timeout_call(timeout, self._search_wikidata, [])
            elif self.search_query[self.current_searching_query_index].search_type == "general":
                search_res = timeout_call(timeout, self._search_datamart, [])
            elif self.search_query[self.current_searching_query_index].search_type == "vector":
                search_res = timeout_call(timeout, self._search_vector, [])
            elif self.search_query[self.current_searching_query_index].search_type == "geospatial":
                search_res = timeout_call(timeout, self._search_geospatial_data, [])
            else:
                raise ValueError("Unknown search query type for " +
                                 self.search_query[self.current_searching_query_index].search_type)

            time_used = (time.time() - time_start)
            timeout -= time_used
            if search_res is not None:
                self._logger.info("Running search on query No." + str(self.current_searching_query_index) + " used "
                                  + str(time_used) + " seconds and finished.")
                self._logger.info("Remained searching time: " + str(timeout) + " seconds.")
            elif timeout <= 0:
                self._logger.error(
                    "Running search on query No." + str(self.current_searching_query_index) + " timeout!")
                break
            else:
                self._logger.error("Running search on query No." + str(self.current_searching_query_index) + " failed!")

            self.current_searching_query_index += 1
            if search_res is not None:
                current_result.extend(search_res)

        if len(current_result) == 0:
            self._logger.warning("No search results found!")
            return None
        else:
            if len(current_result) > limit:
                self.remained_part = current_result[limit:]
                current_result = current_result[:limit]
            return current_result

    def _check_need_wikifier_or_not(self) -> bool:
        """
        Check whether need to run wikifier or not, if wikidata type column detected, this column's semantic type will also be
        checked if no Q node semantic exist
        :return: a bool value
        True means Q nodes column already detected and skip running wikifier
        False means no Q nodes column detected, need to run wikifier
        """
        need_wikifier_or_not, self.supplied_data = d3m_wikifier.check_and_correct_q_nodes_semantic_type(self.supplied_data)
        if not need_wikifier_or_not:
            # if not need to run wikifier, we can find q node columns now
            self.q_nodes_columns = self._find_q_node_columns()
        return need_wikifier_or_not

    def _find_q_node_columns(self) -> typing.List[int]:
        """
        Inner function used to find q node columns by semantic type
        :return: a list of int which indcate the q node columns
        """
        q_nodes_columns = list()
        if type(self.supplied_data) is d3m_Dataset:
            res_id, supplied_dataframe = d3m_utils.get_tabular_resource(dataset=self.supplied_data, resource_id=None)
            selector_base_type = "ds"
        else:
            supplied_dataframe = self.supplied_data
            selector_base_type = "df"

        # check whether Qnode is given in the inputs, if given, use this to search
        metadata_input = self.supplied_data.metadata
        for i in range(supplied_dataframe.shape[1]):
            if selector_base_type == "ds":
                metadata_selector = (res_id, ALL_ELEMENTS, i)
            else:
                metadata_selector = (ALL_ELEMENTS, i)
            if Q_NODE_SEMANTIC_TYPE in metadata_input.query(metadata_selector)["semantic_types"]:
                # if no required variables given, attach any Q nodes found
                q_nodes_columns.append(i)
        return q_nodes_columns

    def run_wikifier(self, input_data: d3m_Dataset) -> d3m_Dataset:
        """
        function used to run wikifier, and then return a d3m_dataset as the wikified results if success,
        otherwise return original input
        :return: None
        """
        self._logger.debug("Start running wikifier for supplied data in search...")
        results = d3m_wikifier.run_wikifier(supplied_data=input_data)
        self._logger.info("Wikifier running finished.")
        self.need_run_wikifier = False
        self.q_nodes_columns = self._find_q_node_columns()
        return results

    def _search_wikidata(self, query=None, supplied_data: typing.Union[d3m_DataFrame, d3m_Dataset] = None,
                         search_threshold=0.5) -> typing.List["DatamartSearchResult"]:
        """
        The search function used for wikidata search
        :param query: JSON object describing the query.
        :param supplied_data: the data you are trying to augment.
        :param search_threshold: the minimum appeared times of the properties
        :return: list of search results of DatamartSearchResult
        """
        self._logger.debug("Start running search on wikidata...")
        if supplied_data is None:
            supplied_data = self.supplied_data

        wikidata_results = []
        try:
            if type(supplied_data) is d3m_Dataset:
                res_id, supplied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None)
            else:
                supplied_dataframe = supplied_data

            if len(self.q_nodes_columns) == 0:
                self._logger.warning("No wikidata Q nodes detected on corresponding required_variables!")
                self._logger.warning("Will skip wikidata search part")
                return wikidata_results

            else:
                self._logger.info("Wikidata Q nodes inputs detected! Will search with it.")
                self._logger.info("Totally " + str(len(self.q_nodes_columns)) + " Q nodes columns detected!")

                # do a wikidata search for each Q nodes column
                for each_column in self.q_nodes_columns:
                    self._logger.debug("Start searching on column " + str(each_column))
                    q_nodes_list = supplied_dataframe.iloc[:, each_column].tolist()
                    p_count = collections.defaultdict(int)
                    p_nodes_needed = []
                    # old method, the generated results are not very good
                    """
                    http_address = 'http://minds03.isi.edu:4444/get_properties'
                    headers = {"Content-Type": "application/json"}
                    requests_data = str(q_nodes_list)
                    requests_data = requests_data.replace("'", '"')
                    r = requests.post(http_address, data=requests_data, headers=headers)
                    results = r.json()
                    for each_p_list in results.values():
                        for each_p in each_p_list:
                            p_count[each_p] += 1
                    """
                    # TODO: temporary change to call wikidata service, may change back in the future
                    # Q node format (wd:Q23)(wd: Q42)
                    q_node_query_part = ""
                    # ensure every time we get same order of q nodes so the hash tag will be same
                    unique_qnodes = set(q_nodes_list)
                    unique_qnodes = list(unique_qnodes)
                    unique_qnodes.sort()

                    for each in unique_qnodes:
                        if len(each) > 0:
                            q_node_query_part += "(wd:" + each + ")"
                    sparql_query = "select distinct ?item ?property where \n{\n  VALUES (?item) {" + q_node_query_part \
                                   + "  }\n  ?item ?property ?value .\n  ?wd_property wikibase:directClaim ?property ." \
                                   + "  values ( ?type ) \n  {\n    ( wikibase:Quantity )\n" \
                                   + "    ( wikibase:Time )\n    ( wikibase:Monolingualtext )\n  }" \
                                   + "  ?wd_property wikibase:propertyType ?type .\n}\norder by ?item ?property "

                    results = self.wikidata_cache_manager.get_result(sparql_query)

                    if results is None:
                        # if response none, it means get wikidata query results failed
                        self._logger.error("Can't get wikidata search results for column No." + str(each_column) + "(" +
                                           supplied_dataframe.columns[each_column] + ")")
                        continue

                    self._logger.debug("Response from server for column No." + str(each_column) + "(" +
                                       supplied_dataframe.columns[each_column] + ")" +
                                       " received, start parsing the returned data from server.")
                    # count the appeared times and find the p nodes appeared  rate that higher than threshold
                    for each in results:
                        p_count[each['property']['value'].split("/")[-1]] += 1

                    for key, val in p_count.items():
                        if float(val) / len(unique_qnodes) >= search_threshold:
                            p_nodes_needed.append(key)
                    wikidata_search_result = {"p_nodes_needed": p_nodes_needed,
                                              "target_q_node_column_name": supplied_dataframe.columns[each_column]}
                    wikidata_results.append(DatamartSearchResult(search_result=wikidata_search_result,
                                                                 supplied_data=supplied_data,
                                                                 query_json=query,
                                                                 search_type="wikidata")
                                            )

                self._logger.debug("Running search on wikidata finished.")
            return wikidata_results

        except Exception as e:
            self._logger.error("Searching with wikidata failed!")
            self._logger.debug(e, exc_info=True)

        finally:
            return wikidata_results

    def _search_datamart(self) -> typing.List["DatamartSearchResult"]:
        """
        function used for searching in datamart with blaze graph database
        :return: List[DatamartSearchResult]
        """
        self._logger.debug("Start searching on datamart...")
        search_result = []
        variables_search = self.search_query[self.current_searching_query_index].variables_search
        keywords_search = self.search_query[self.current_searching_query_index].keywords_search
        # COMMENT: title does not used, may delete later
        variables, title = dict(), dict()
        variables_temp = dict()  # this temp is specially used to store variable for time query
        for each_variable in self.search_query[self.current_searching_query_index].variables:
            if each_variable.key.startswith(TIME_COLUMN_MARK):
                variables_temp[each_variable.key.split("____")[1]] = each_variable.values
                start_time, end_time, granularity = each_variable.values.split("____")
                variables_search = {"temporal_variable": {
                                         "start": start_time,
                                         "end": end_time,
                                         "granularity": granularity
                                         }
                                    }
            else:
                variables[each_variable.key] = each_variable.values

        query = {"keywords": self.search_query[self.current_searching_query_index].keywords,
                 "variables": variables,
                 "keywords_search": keywords_search,
                 "variables_search": variables_search,
                 }

        query_results = self.augmenter.query_by_sparql(query=query, dataset=self.supplied_data)

        if len(variables_temp) != 0:
            query["variables"] = variables_temp

        for i, each_result in enumerate(query_results):
            self._logger.debug("Get returned No." + str(i) + " query result as ")
            self._logger.debug(str(each_result))

            # the special way to calculate the score of temporal variable search
            if "start_time" in each_result.keys() and "end_time" in each_result.keys():
                tv = query["variables_search"]["temporal_variable"]
                start_date = pd.to_datetime(tv["start"]).timestamp()
                end_date = pd.to_datetime(tv["end"]).timestamp()  # query time
                start_time = pd.to_datetime(each_result['start_time']['value']).timestamp()
                end_time = pd.to_datetime(each_result['end_time']['value']).timestamp()  # dataset

                denominator = float(end_date - start_date)
                if end_date > end_time:
                    if start_date > end_time:
                        time_score = 0.0
                    elif start_date >= start_time and end_time >= start_date:
                        time_score = (end_time - start_date) / denominator
                    elif start_time > start_date:
                        time_score = (end_time - start_time) / denominator
                elif end_date >= start_time and end_time >= end_date:
                    if start_date >= start_time:
                        time_score = 1.0
                    elif start_time > start_date:
                        time_score = (end_date - start_time) / denominator
                elif start_time > end_date:
                    time_score = 0.0

                if time_score != 0.0 and 'score' in each_result.keys():
                    old_score = float(each_result['score']['value'])
                    each_result['score']['value'] = (old_score + time_score) / 2
                else:
                    each_result['score'] = {"value": time_score}

            temp = DatamartSearchResult(search_result=each_result, supplied_data=self.supplied_data, query_json=query,
                                        search_type="general")
            search_result.append(temp)

        search_result.sort(key=lambda x: x.score(), reverse=True)

        self._logger.debug("Searching on datamart finished.")
        return search_result

    def _search_vector(self) -> typing.List["DatamartSearchResult"]:
        """
        The search function used for vector search
        :return: List[DatamartSearchResult]
        """
        self._logger.debug("Start running search on Vectors...")
        vector_results = []
        try:
            if type(self.supplied_data) is d3m_Dataset:
                res_id, supplied_dataframe = d3m_utils.get_tabular_resource(dataset=self.supplied_data, resource_id=None)
            else:
                supplied_dataframe = self.supplied_data

            if len(self.q_nodes_columns) == 0:
                self._logger.warning("No Wikidata Q nodes detected!")
                self._logger.warning("Will skip vector search part")
                return vector_results
            else:
                self._logger.info("Wikidata Q nodes inputs detected! Will search with it.")
                self._logger.info("Totally " + str(len(self.q_nodes_columns)) + " Q nodes columns detected!")

                # do a vector search for each Q nodes column
                for each_column in self.q_nodes_columns:
                    self._logger.debug("Start searching on column " + str(each_column))
                    q_nodes_list = list(filter(None, supplied_dataframe.iloc[:, each_column].dropna().tolist()))
                    unique_qnodes = list(set(q_nodes_list))
                    unique_qnodes.sort()

                    vector_search_result = {"number_of_vectors": str(len(unique_qnodes)),
                                            "target_q_node_column_name": supplied_dataframe.columns[each_column],
                                            "q_nodes_list": unique_qnodes}
                    vector_results.append(DatamartSearchResult(search_result=vector_search_result,
                                                               supplied_data=self.supplied_data,
                                                               query_json=None,
                                                               search_type="vector")
                                            )

                self._logger.debug("Running search on vector finished.")
            return vector_results
        except Exception as e:
            self._logger.error("Searching with wikidata vector failed!")
            self._logger.debug(e, exc_info=True)

        finally:
            return vector_results

    def _search_geospatial_data(self) -> typing.List["DatamartSearchResult"]:
        """
        function used for searching geospatial data
        :return: List[DatamartSearchResult]
        """
        self._logger.debug("Start searching geospatial data on wikidata and datamart...")
        search_results = []

        if type(self.supplied_data) is d3m_Dataset:
            res_id, supplied_dataframe = d3m_utils.get_tabular_resource(dataset=self.supplied_data, resource_id=None)
        else:
            supplied_dataframe = self.supplied_data

        # try to find possible columns of latitude and longitude
        possible_longitude_or_latitude = []
        for each in range(len(supplied_dataframe.columns)):
            if type(self.supplied_data) is d3m_Dataset:
                selector = (res_id, ALL_ELEMENTS, each)
            else:
                selector = (ALL_ELEMENTS, each)
            each_column_meta = self.supplied_data.metadata.query(selector)

            if "https://metadata.datadrivendiscovery.org/types/Location" in each_column_meta["semantic_types"]:
                try:
                    column_data = supplied_dataframe.iloc[:, each].astype(float).dropna()
                    if max(column_data) <= config.max_longitude_val and min(column_data) >= config.min_longitude_val:
                        possible_longitude_or_latitude.append(each)
                    elif max(column_data) <= config.max_latitude_val and min(column_data) >= config.min_latitude_val:
                        possible_longitude_or_latitude.append(each)
                except:
                    pass

        if len(possible_longitude_or_latitude) < 2:
            self._logger.debug("Supplied dataset does not have geospatial data!")
            return search_results
        else:
            self._logger.debug("Finding columns:" + str(possible_longitude_or_latitude) + " which might be geospatial data columns...")

        possible_la_or_long_comb = list(combinations(possible_longitude_or_latitude, 2))
        for column_index_comb in possible_la_or_long_comb:
            latitude_index, longitude_index = -1 , -1
            # try to get the correct latitude and longitude pairs
            for each_column_index in column_index_comb:
                try:
                    column_data = supplied_dataframe.iloc[:, each_column_index].astype(float).dropna()
                    column_name = supplied_dataframe.columns[each_column_index]

                    # must be longitude when its min is in [-180, -90), or max is in (90, 180]
                    if config.max_latitude_val < max(column_data) <= config.max_longitude_val \
                            or (config.min_latitude_val > min(column_data) >= config.min_longitude_val):
                        longitude_index = each_column_index
                    else:
                        # determine the type by header [latitude, longitude]
                        if any([True for i in column_name if i in ['a', 'A']]):
                            latitude_index = each_column_index
                        elif any([True for i in column_name if i in ['o', 'O', 'g', 'G']]):
                            longitude_index = each_column_index

                except Exception as e:
                    self._logger.debug(e, exc_info=True)
                    self._logger.error("Can't parse location information for column No." + str(each_column_index)
                                           + " with column name " + column_name)

            # search on datamart and wikidata by city qnodes
            if latitude_index != -1 and longitude_index != -1:
                self._logger.info("Latitude column is: " + str(latitude_index) + " and longitude is: " + str(longitude_index) + "...")
                granularity = {'city'}
                radius = 100

                for gran in granularity:
                    search_variables = {'metadata': {
                        'search_result': {
                            'latitude_index': latitude_index,
                            'longitude_index': longitude_index,
                            'radius': radius,
                            'granularity': gran
                        },
                        'search_type': 'geospatial'
                    }}
                    # do wikidata query service to find city q-node columns
                    return_ds = DownloadManager.query_geospatial_wikidata(self.supplied_data, search_variables, self.connection_url)
                    _, return_df = d3m_utils.get_tabular_resource(dataset=return_ds, resource_id=None)

                    if return_df.columns[-1].startswith('Geo_') and return_df.columns[-1].endswith('_wikidata'):
                        qnodes = return_df.iloc[:, -1]
                        qnodes_set = list(set(qnodes))
                        coverage_score = len(qnodes_set)/len(qnodes)

                        # search on datamart
                        qnodes_str = " ".join(qnodes_set)
                        variables = [VariableConstraint(key=return_df.columns[-1], values=qnodes_str)]
                        self.search_query[self.current_searching_query_index].variables = variables
                        search_res = timeout_call(1800, self._search_datamart, [])
                        search_results.extend(search_res)

                        # search on wikidata
                        temp_q_nodes_columns = self.q_nodes_columns
                        self.q_nodes_columns = [-1]
                        search_res = timeout_call(1800, self._search_wikidata, [None, return_df])
                        search_results.extend(search_res)
                        self.q_nodes_columns = temp_q_nodes_columns

        if search_results:
            for each_result in search_results:
                # change metadata's score
                old_score = each_result.score()
                new_score = old_score * coverage_score
                each_result.metadata_manager.score = new_score
                # change score in datamart_search_result
                if "score" in each_result.search_result.keys():
                    each_result.search_result["score"]["value"] = new_score

            search_results.sort(key=lambda x: x.score(), reverse=True)

        self._logger.debug("Running search on geospatial data finished.")
        return search_results


class Datamart(object):
    """
    ISI implement of datamart
    """

    def __init__(self, connection_url: str=None) -> None:
        self._logger = logging.getLogger(__name__)
        if connection_url:
            self._logger.info("Using user-defined connection url as " + connection_url)
            self.connection_url = connection_url
        else:
            # TODO: currently temporary add also to get nyu's datamart url here, should set to use isi's in the future
            connection_url = os.getenv('DATAMART_URL_NYU', DEFAULT_DATAMART_URL)
            self.connection_url = connection_url

        self._logger.debug("Current datamart connection url is: " + self.connection_url)
        self.augmenter = Augment(endpoint=self.connection_url)
        self.supplied_dataframe = None

    def search(self, query: 'DatamartQuery') -> DatamartQueryCursor:
        """This entry point supports search using a query specification.

        The query specification supports querying datasets by keywords, named entities, temporal ranges, and geospatial ranges.

        Datamart implementations should return a DatamartQueryCursor immediately.

        Parameters
        ----------
        query : DatamartQuery
            Query specification.

        Returns
        -------
        DatamartQueryCursor
            A cursor pointing to search results.
        """

        return DatamartQueryCursor(augmenter=self.augmenter, search_query=[query], supplied_data=None,
                                   connection_url=self.connection_url, need_run_wikifier = False)

    def search_with_data(self, query: 'DatamartQuery', supplied_data: container.Dataset, need_wikidata=True) \
            -> DatamartQueryCursor:
        """
        Search using on a query and a supplied dataset.

        This method is a "smart" search, which leaves the Datamart to determine how to evaluate the relevance of search
        result with regard to the supplied data. For example, a Datamart may try to identify named entities and date
        ranges in the supplied data and search for companion datasets which overlap.

        To manually specify query constraints using columns of the supplied data, use the `search_with_data_columns()`
        method and `TabularVariable` constraints.

        Datamart implementations should return a DatamartQueryCursor immediately.

        Parameters
        ------_---
        query : DatamartQuery
            Query specification
        supplied_data : container.Dataset
            The data you are trying to augment.

        Returns
        -------
        DatamartQueryCursor
            A cursor pointing to search results containing possible companion datasets for the supplied data.
        """

        # first take a search on wikidata
        # add wikidata searching query at first position
        res_id = None
        if not need_wikidata:
            search_queries = []
            need_run_wikifier = False
        else:
            need_run_wikifier = None
            search_queries = [DatamartQuery(search_type="wikidata"), DatamartQuery(search_type="vector"), DatamartQuery(search_type="geospatial")]

        # try to update with more correct metadata if possible
        updated_result = MetadataCache.check_and_get_dataset_real_metadata(supplied_data)
        if updated_result[0]:  # [0] store whether it success find the metadata
            supplied_data = updated_result[1]

        if type(supplied_data) is d3m_Dataset:
            res_id, self.supplied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None)
        else:
            raise ValueError("Incorrect supplied data type as " + str(type(supplied_data)))

        # if query is None:
        # if not query given, try to find the Text columns from given dataframe and use it to find some candidates
        can_query_columns = []
        for each in range(len(self.supplied_dataframe.columns)):
            if type(supplied_data) is d3m_Dataset:
                selector = (res_id, ALL_ELEMENTS, each)
            else:
                selector = (ALL_ELEMENTS, each)
            each_column_meta = supplied_data.metadata.query(selector)
            # try to parse each column to DateTime type. If success, add new semantic type, otherwise do nothing
            try:
                pd.to_datetime(self.supplied_dataframe.iloc[:, each])
                new_semantic_type = {"semantic_types": ("http://schema.org/DateTime",
                                                        "https://metadata.datadrivendiscovery.org/types/Attribute")}
                supplied_data.metadata = supplied_data.metadata.update(selector, new_semantic_type)
            except:
                pass

            if 'http://schema.org/Text' in each_column_meta["semantic_types"] \
                    or "http://schema.org/DateTime" in each_column_meta["semantic_types"]:
                can_query_columns.append(each)

        if len(can_query_columns) == 0:
            self._logger.warning("No column can be used for augment with datamart!")

        for each_column_index in can_query_columns:
            column_formated = DatasetColumn(res_id, each_column_index)
            tabular_variable = TabularVariable(columns=[column_formated], relationship=ColumnRelationship.CONTAINS)
            each_search_query = self.generate_datamart_query_from_data(supplied_data=supplied_data,
                                                                       data_constraints=[tabular_variable])
            search_queries.append(each_search_query)

        return DatamartQueryCursor(augmenter=self.augmenter, search_query=search_queries, supplied_data=supplied_data,
                                   need_run_wikifier=need_run_wikifier, connection_url=self.connection_url)

    def search_with_data_columns(self, query: 'DatamartQuery', supplied_data: container.Dataset,
                                 data_constraints: typing.List['TabularVariable']) -> DatamartQueryCursor:
        """
        Search using a query which can include constraints on supplied data columns (TabularVariable).

        This search is similar to the "smart" search provided by `search_with_data()`, but caller must manually specify
        constraints using columns from the supplied data; Datamart will not automatically analyze it to determine
        relevance or joinability.

        Use of the query spec enables callers to compose their own "smart search" implementations.

        Datamart implementations should return a DatamartQueryCursor immediately.

        Parameters
        ------_---
        query : DatamartQuery
            Query specification
        supplied_data : container.Dataset
            The data you are trying to augment.
        data_constraints : list
            List of `TabularVariable` constraints referencing the supplied data.

        Returns
        -------
        DatamartQueryCursor
            A cursor pointing to search results containing possible companion datasets for the supplied data.
        """

        # put entities of all given columns from "data_constraints" into the query's variable part and run the query

        # try to update with more correct metadata if possible
        updated_result = MetadataCache.check_and_get_dataset_real_metadata(supplied_data)
        if updated_result[0]:  # [0] store whether it success find the metadata
            supplied_data = updated_result[1]

        search_query = self.generate_datamart_query_from_data(supplied_data=supplied_data,
                                                              data_constraints=data_constraints)
        return DatamartQueryCursor(augmenter=self.augmenter, search_query=[search_query], supplied_data=supplied_data,
                                   connection_url=self.connection_url)

    def generate_datamart_query_from_data(self, supplied_data: container.Dataset,
                                          data_constraints: typing.List['TabularVariable']) -> "DatamartQuery":
        """
        Inner function used to generate the isi implemented datamart query from given dataset
        :param supplied_data: a Dataset format supplied data
        :param data_constraints:
        :return: a DatamartQuery can be used in isi datamart
        """
        all_query_variables = []
        keywords = []
        translator = str.maketrans(string.punctuation, ' ' * len(string.punctuation))

        for each_constraint in data_constraints:
            for each_column in each_constraint.columns:
                each_column_index = each_column.column_index
                each_column_res_id = each_column.resource_id
                all_value_str_set = set()
                each_column_meta = supplied_data.metadata.query((each_column_res_id, ALL_ELEMENTS, each_column_index))
                treat_as_a_text_column = False
                if 'http://schema.org/DateTime' in each_column_meta["semantic_types"]:
                    try:
                        column_data = supplied_data[each_column_res_id].iloc[:, each_column_index]
                        column_data_datetime_format = pd.to_datetime(column_data)
                        start_date = min(column_data_datetime_format)
                        end_date = max(column_data_datetime_format)
                        if any(column_data_datetime_format.dt.second != 0):
                            time_granularity = 'second'
                        elif any(column_data_datetime_format.dt.minute != 0):
                            time_granularity = 'minute'
                        elif any(column_data_datetime_format.dt.hour != 0):
                            time_granularity = 'hour'
                        elif any(column_data_datetime_format.dt.day != 0):
                            time_granularity = 'day'
                        elif any(column_data_datetime_format.dt.month != 0):
                            time_granularity = 'month'
                        elif any(column_data_datetime_format.dt.year != 0):
                            time_granularity = 'year'
                        else:
                            self._logger.error("No usefully date information found for column No." + str(each_column_index))
                            raise ValueError("No usefully date information found for column No." + str(each_column_index))

                        # for time type, we create a special type of keyword and variables
                        # so that we can detect it later in general search part
                        each_keyword = TIME_COLUMN_MARK + "____" + supplied_data[each_column_res_id].columns[each_column_index]
                        keywords.append(each_keyword)
                        all_value_str = str(start_date) + "____" + str(end_date) + "____" + time_granularity
                        all_query_variables.append(VariableConstraint(key=each_keyword, values=all_value_str))

                    except Exception as e:
                        self._logger.debug(e, exc_info=True)
                        self._logger.error("Can't parse current datetime for column No." + str(each_column_index)
                                           + " with column name " + supplied_data[each_column_res_id].columns[each_column_index])
                        treat_as_a_text_column = True

                # for some special condition (DA_medical_malpractice), a column could have a DateTime tag but unable to be parsed
                # in such condition, we should search and treat it as a Text column then
                if 'http://schema.org/Text' in each_column_meta["semantic_types"] or treat_as_a_text_column:
                    column_values = supplied_data[each_column_res_id].iloc[:, each_column_index].astype(str)
                    query_column_entities = list(set(column_values.tolist()))
                    if len(query_column_entities) > MAX_ENTITIES_LENGTH:
                        query_column_entities = random.sample(query_column_entities, MAX_ENTITIES_LENGTH)
                    for each in query_column_entities:
                        words_processed = str(each).lower().translate(translator).split()
                        for word in words_processed:
                            all_value_str_set.add(word)
                    all_value_str = " ".join(all_value_str_set)
                    each_keyword = supplied_data[each_column_res_id].columns[each_column_index]
                    keywords.append(each_keyword)

                    all_query_variables.append(VariableConstraint(key=each_keyword, values=all_value_str))

        search_query = DatamartQuery(keywords=keywords, variables=all_query_variables)

        return search_query


class DatasetColumn:
    """
    Specify a column of a dataframe in a D3MDataset
    """

    def __init__(self, resource_id: str, column_index: int) -> None:
        self.resource_id = resource_id
        self.column_index = column_index


class DatamartSearchResult:
    """
    This class represents the search results of a datamart search.
    Different datamarts will provide different implementations of this class.
    """

    def __init__(self, search_result, supplied_data, query_json, search_type, connection_url=None):
        self._logger = logging.getLogger(__name__)
        self.search_result = search_result
        self.supplied_data = supplied_data
        if type(supplied_data) is d3m_Dataset:
            self.res_id, self.supplied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data,
                                                                                  resource_id=None)
            self.selector_base_type = "ds"
        elif type(supplied_data) is d3m_DataFrame:
            self.supplied_dataframe = supplied_data
            self.selector_base_type = "df"
        else:
            self.supplied_dataframe = None

        if connection_url:
            self._logger.info("Using user-defined connection url as " + connection_url)
            self.connection_url = connection_url
        else:
            # TODO: currently temporary add also to get nyu's datamart url here, should set to use isi's in the future
            connection_url = os.getenv('DATAMART_URL_NYU', DEFAULT_DATAMART_URL)
            self.connection_url = connection_url

        self.wikidata_cache_manager = QueryCache(connection_url=self.connection_url)
        self.general_search_cache_manager = GeneralSearchCache(connection_url=self.connection_url)
        self.query_json = query_json
        self.search_type = search_type
        self.pairs = None
        self._res_id = None  # only used for input is Dataset
        self.join_pairs = None
        self.right_df = None
        self.metadata_manager = MetadataGenerator(supplied_data=self.supplied_data, search_result=self.search_result,
                                                  search_type=self.search_type, connection_url=self.connection_url,
                                                  wikidata_cache_manager=self.wikidata_cache_manager)
        self.d3m_metadata = self.metadata_manager.generate_d3m_metadata_for_search_result()

    def display(self) -> pd.DataFrame:
        """
        function used to see what found inside this search result class in a human vision
        contains information for search result's title, columns and join hints
        :return: a pandas DataFrame
        """
        return self.metadata_manager.get_simple_view()

    def download(self, supplied_data: typing.Union[d3m_Dataset, d3m_DataFrame] = None,
                 connection_url: str = None, generate_metadata=True, return_format="ds", run_wikifier=True) \
            -> typing.Union[container.Dataset, container.DataFrame]:
        """
        Produces a D3M dataset (data plus metadata) corresponding to the search result.
        Every time the download method is called on a search result, it will produce the exact same columns
        (as specified in the metadata -- get_metadata), but the set of rows may depend on the supplied_data.
        Datamart is encouraged to return a dataset that joins well with the supplied data, e.g., has rows that match
        the entities in the supplied data. Datamarts may ignore the supplied_data and return the same data regardless.

        If the supplied_data is None, Datamarts may return None or a default dataset, based on the search query.

        Parameters
        ---------
        supplied_data : container.Dataset
            A D3M dataset containing the dataset that is the target for augmentation. Datamart will try to download data
            that augments the supplied data well.
        connection_url : str
            A connection string used to connect to a specific Datamart deployment. If not provided, the one provided to
            the `Datamart` constructor is used.
        generate_metadata: bool
            Whether need to get the auto-generated metadata or not, only valid in isi datamart
        return_format: str
            A control parameter to set which type of output should get, the default value is "ds" as dataset
            Optional choice is to get dataframe type output. Only valid in isi datamart
        run_wikifier： str
            A control parameter to set whether to run wikifier on this search result
        """
        if connection_url:
            # if a new connection url given
            if self.connection_url != connection_url:
                self.connection_url = connection_url
                self.wikidata_cache_manager = QueryCache(connection_url=self.connection_url)
                self.general_search_cache_manager = GeneralSearchCache(connection_url=self.connection_url)
                self.metadata_manager = MetadataGenerator(supplied_data=supplied_data, search_result=self.search_result,
                                                          search_type=self.search_type, connection_url=connection_url,
                                                          wikidata_cache_manager=self.wikidata_cache_manager)
                self._logger.info("New connection url given from download part as " + self.connection_url)

        if type(supplied_data) is d3m_Dataset:
            self._res_id, self.supplied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None)
        elif type(supplied_data) is d3m_DataFrame:
            self.supplied_dataframe = supplied_data
        else:
            self._logger.warning("No supplied data given, will try to use the exist one")
            if self.supplied_dataframe is None and self.supplied_data is None:
                raise ValueError("No supplied data found!")

        # get the results without metadata
        if self.search_type == "general":
            res = self._download_general(run_wikifier=run_wikifier)
        elif self.search_type == "wikidata":
            res = self._download_wikidata()
        elif self.search_type == "vector":
            res = self._download_vector()
        else:
            raise ValueError("Unknown search type with " + self.search_type)

        # sometime the index will be not continuous after augment, need to reset to ensure the index is continuous
        res.reset_index(drop=True)

        if return_format == "ds":
            return_df = d3m_DataFrame(res, generate_metadata=False)
            resources = {AUGMENT_RESOURCE_ID: return_df}
            return_result = d3m_Dataset(resources=resources, generate_metadata=False)
        elif return_format == "df":
            return_result = d3m_DataFrame(res, generate_metadata=False)
        else:
            raise ValueError("Invalid return format was given as " + str(return_format))

        if generate_metadata:
            return_result = self.metadata_manager.generate_metadata_for_download_result(return_result, supplied_data)

        return return_result

    def _download_general(self, run_wikifier) -> pd.DataFrame:
        """
        Specified download function for general datamart Datasets
        :return: a dataset or a dataframe depending on the input
        """
        self._logger.debug("Start downloading for datamart...")

        join_pairs_result = []
        candidate_join_column_scores = []

        # start finding pairs
        left_df = copy.deepcopy(self.supplied_dataframe)
        if self.right_df is None:
            self.right_df = Utils.materialize(metadata=self.search_result, run_wikifier=run_wikifier)
            right_df = self.right_df
        else:
            self._logger.info("Find downloaded data from previous time, will use that.")
            right_df = self.right_df
        self._logger.debug("Download finished, start generating d3m metadata.")
        left_metadata = Utils.generate_metadata_from_dataframe(data=left_df, original_meta=None)
        right_metadata = Utils.generate_metadata_from_dataframe(data=right_df, original_meta=None)

        if self.join_pairs is None:
            candidate_join_column_pairs = self.get_join_hints(left_df=left_df, right_df=right_df, left_df_src_id=self._res_id)
        else:
            candidate_join_column_pairs = self.join_pairs
        if len(candidate_join_column_pairs) > 1:
            logging.warning("multiple joining column pairs found! Will only check first one.")
        elif len(candidate_join_column_pairs) < 1:
            logging.error("Getting joining pairs failed")

        is_time_query = False
        if "start_time" in self.search_result and "end_time" in self.search_result:
            for each in self.query_json['keywords']:
                if TIME_COLUMN_MARK in each:
                    is_time_query = True
                    break

        if is_time_query:
            # if it is the dataset fond with time query, we should transform that time column to same format
            # then we can run RLTK with exact join same as str join
            right_join_column_name = self.search_result['variableName']['value']
            right_df[right_join_column_name] = pd.to_datetime(right_df[right_join_column_name])

        pairs = candidate_join_column_pairs[0].get_column_number_pairs()
        # generate the pairs for each join_column_pairs
        for each_pair in pairs:
            left_columns = each_pair[0]
            right_columns = each_pair[1]
            try:
                # Only profile the joining columns, otherwise it will be too slow:
                left_metadata = Utils.calculate_dsbox_features(data=left_df, metadata=left_metadata,
                                                               selected_columns=set(left_columns))

                right_metadata = Utils.calculate_dsbox_features(data=right_df, metadata=right_metadata,
                                                                selected_columns=set(right_columns))

                self._logger.info(" - start getting pairs for " + str(each_pair))
                right_df_copy = copy.deepcopy(right_df)

                result, self.pairs = RLTKJoinerGeneral.find_pair(left_df=left_df, right_df=right_df_copy,
                                                                 left_columns=[left_columns],
                                                                 right_columns=[right_columns],
                                                                 left_metadata=left_metadata,
                                                                 right_metadata=right_metadata)

                join_pairs_result.append(result)
                # TODO: figure out some way to compute the joining quality
                candidate_join_column_scores.append(1)

            except Exception as e:
                self._logger.error("failed when getting pairs for", each_pair)
                self._logger.debug(e, exc_info=True)

        # choose the best joining results
        all_results = []
        for i in range(len(join_pairs_result)):
            each_result = (pairs[i], candidate_join_column_scores[i], join_pairs_result[i])
            all_results.append(each_result)

        all_results.sort(key=lambda x: x[1], reverse=True)
        if len(all_results) == 0:
            raise ValueError("All join attempt failed!")

        return_result = all_results[0][2]
        self._logger.debug("download_general function finished.")
        return return_result

    def _download_wikidata(self) -> pd.DataFrame:
        """
        :return: return_df: the materialized wikidata d3m_DataFrame,
                            with corresponding pairing information to original_data at last column
        """
        self._logger.debug("Start downloading for wikidata...")
        # prepare the query
        p_nodes_needed = self.search_result["p_nodes_needed"]
        target_q_node_column_name = self.search_result["target_q_node_column_name"]

        try:
            q_node_column_number = self.supplied_dataframe.columns.tolist().index(target_q_node_column_name)
        except ValueError:
            raise ValueError("Could not find corresponding q node column for " + target_q_node_column_name +
                             ". Maybe use the wrong search results?")
        q_nodes_list = set(self.supplied_dataframe.iloc[:, q_node_column_number].tolist())
        q_nodes_list = list(q_nodes_list)
        q_nodes_list.sort()
        p_nodes_needed.sort()
        q_nodes_query = ""
        p_nodes_query_part = ""
        p_nodes_optional_part = ""
        special_request_part = ""

        for each in q_nodes_list:
            if each != "N/A":
                q_nodes_query += "(wd:" + each + ") \n"
        for each in p_nodes_needed:
            if each not in P_NODE_IGNORE_LIST:
                p_nodes_query_part += " ?" + each
                p_nodes_optional_part += "  OPTIONAL { ?q wdt:" + each + " ?" + each + "}\n"
            if each in SPECIAL_REQUEST_FOR_P_NODE:
                special_request_part += SPECIAL_REQUEST_FOR_P_NODE[each] + "\n"

        sparql_query = "SELECT DISTINCT ?q " + p_nodes_query_part + \
                       " \nWHERE \n{\n  VALUES (?q) { \n " + q_nodes_query + "}\n" + \
                       p_nodes_optional_part + special_request_part + "}\n"

        results = self.wikidata_cache_manager.get_result(sparql_query)
        return_df = d3m_DataFrame()

        # if results is None, it means download failed, return blank dataFrame directly
        if results is None:
            # print 3 times to ensure easy to find
            self._logger.error("Download failed!!!")
            self._logger.error("Download failed!!!")
            self._logger.error("Download failed!!!")
            return return_df

        q_node_name_appeared = set()
        for result in results:
            each_result = {}
            q_node_name = result.pop("q")["value"].split("/")[-1]
            if q_node_name in q_node_name_appeared:
                continue
            q_node_name_appeared.add(q_node_name)
            each_result["q_node"] = q_node_name
            for p_name, p_val in result.items():
                each_result[p_name] = p_val["value"]
            return_df = return_df.append(each_result, ignore_index=True)

        column_name_update = dict()

        # for some special condition, we may meet this, and we need to ensure q_node column is the last column
        if return_df.columns[-1] != "q_node":
            cols = return_df.columns.tolist()
            cols.append(cols.pop(cols.index("q_node")))
            return_df = return_df[cols]

        # rename the columns from P node value to real name
        i = 0
        while len(self.d3m_metadata.query((ALL_ELEMENTS, i)).keys()) != 0:
            column_meta = self.d3m_metadata.query((ALL_ELEMENTS, i))
            if "P_node" in column_meta:
                column_name_update[column_meta['P_node']] = column_meta['name']
            i += 1

        return_df = return_df.rename(columns=column_name_update)

        # use rltk joiner to find the joining pairs
        joiner = RLTKJoinerWikidata()
        joiner.set_join_target_column_names((self.supplied_dataframe.columns[q_node_column_number], "q_node"))
        result, self.pairs = joiner.find_pair(left_df=self.supplied_dataframe, right_df=return_df)

        self._logger.debug("download_wikidata function finished.")
        return result

    def _download_vector(self) -> pd.DataFrame:
        """
        :return: return_df: the materialized vector d3m_DataFrame,
                            with corresponding pairing information to original_data at last column
        """
        self._logger.debug("Start downloading for vector...")
        target_q_node_column_name = self.search_result["target_q_node_column_name"]

        try:
            q_node_column_number = self.supplied_dataframe.columns.tolist().index(target_q_node_column_name)
        except ValueError:
            raise ValueError("Could not find corresponding q node column for " + target_q_node_column_name +
                             ". Maybe use the wrong search results?")
        q_nodes_list = set(self.supplied_dataframe.iloc[:, q_node_column_number].tolist())
        q_nodes_list = list(q_nodes_list)
        q_nodes_list.sort()

        return_df = DownloadManager.fetch_fb_embeddings(q_nodes_list, target_q_node_column_name)
        return_df = d3m_DataFrame(return_df)

        # use rltk joiner to find the joining pairs
        joiner = RLTKJoinerWikidata()
        joiner.set_join_target_column_names((self.supplied_dataframe.columns[q_node_column_number], "q_node"))
        result, self.pairs = joiner.find_pair(left_df=self.supplied_dataframe, right_df=return_df)

        self._logger.debug("download_vector function finished.")
        return result

    def _run_wikifier(self, supplied_data) -> d3m_Dataset:
        """
        Inner function to do wikifier type augment, this is purposed for doing augment with d3m primitive
        :param supplied_data:
        :return: a wikifiered d3m_Dataset if success
        """
        self._logger.debug("Start running wikifier.")
        # here because this part's code if for augment, we already have cache for that
        results = d3m_wikifier.run_wikifier(supplied_data=supplied_data, use_cache=True)
        self._logger.debug("Running wikifier finished.")
        return results

    def augment(self, supplied_data, augment_columns=None, connection_url=None, augment_resource_id=AUGMENT_RESOURCE_ID):
        """
        Produces a D3M dataset that augments the supplied data with data that can be retrieved from this search result.
        The augment methods is a baseline implementation of download plus augment.

        Callers who want to control over the augmentation process should use the download method and use their own
        augmentation algorithm.

        This function actually do the concat steps that combine the joining pairs found from download and
        return a dataset with more columns. The detail pairs finding algorithm is located in download part
        Parameters
        ---------
        supplied_data : container.Dataset
            A D3M dataset containing the dataset that is the target for augmentation.
        augment_columns : typing.List[DatasetColumn]
            If provided, only the specified columns from the Datamart dataset that will be added to the supplied dataset.
        connection_url : str
            A connection string used to connect to a specific Datamart deployment. If not provided, a different
            deployment might be used.
        augment_resource_id: str
            The augmented dataframe's resource id in return dataset
        """

        if type(supplied_data) is d3m_Dataset:
            # try to update with more correct metadata if possible
            updated_result = MetadataCache.check_and_get_dataset_real_metadata(supplied_data)
            if updated_result[0]:  # [0] store whether it success find the metadata
                supplied_data = updated_result[1]
            self.supplied_data = supplied_data
            self._res_id, self.supplied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data,
                                                                                   resource_id=None,
                                                                                   has_hyperparameter=False)

        else:
            self.supplied_dataframe = supplied_data

        if connection_url:
            self._logger.info("Using user-defined connection url as " + connection_url)
            self.connection_url = connection_url
        else:
            # TODO: currently temporary add also to get nyu's datamart url here, should set to use isi's in the future
            connection_url = os.getenv('DATAMART_URL_NYU', DEFAULT_DATAMART_URL)
            self.connection_url = connection_url

        try:
            cache_key = self.general_search_cache_manager.get_hash_key(supplied_dataframe=self.supplied_dataframe,
                                                                       search_result_serialized=self.serialize())
            cache_result = self.general_search_cache_manager.get_cache_results(cache_key)
            if cache_result is not None:
                if type(cache_result) is string and cache_result == "failed":
                    self._logger.warning("This augment was failed last time!")
                self._logger.info("Using caching results")
                return cache_result

        except Exception as e:
            cache_key = None
            self._logger.error("Some error happened when getting results from cache!")
            self._logger.debug(e, exc_info=True)

        self._logger.info("Cache not hit, start running augment.")

        try:
            if self.search_type == "wikifier":
                res = timeout_call(1800, self._run_wikifier, [supplied_data])
                # res = self._run_wikifier(supplied_data)

            else:
                if type(supplied_data) is d3m_DataFrame:
                    res = timeout_call(1800, self._augment, [supplied_data, augment_columns, True, "df", augment_resource_id])

                    # res = self._augment(supplied_data=supplied_data, augment_columns=augment_columns, generate_metadata=True,
                    #                     return_format="df", augment_resource_id=augment_resource_id)
                elif type(supplied_data) is d3m_Dataset:
                    res = timeout_call(1800, self._augment, [supplied_data, augment_columns, True, "ds", augment_resource_id])
                    # res = self._augment(supplied_data=supplied_data, augment_columns=augment_columns, generate_metadata=True,
                    #                     return_format="ds", augment_resource_id=augment_resource_id)
                else:
                    raise ValueError("Unknown input type for supplied data as: " + str(type(supplied_data)))

            if res is not None:
                # sometime the index will be not continuous after augment, need to reset to ensure the index is continuous
                res[augment_resource_id].reset_index(drop=True)

                res[augment_resource_id].fillna('', inplace=True)
                res[augment_resource_id] = res[augment_resource_id].astype(str)
            else:
                res = "failed"

        except Exception as e:
            self._logger.error("Augment failed!")
            self._logger.debug(e, exc_info=True)
            res = "failed"

        # should not cache wikifier results here, as we already cached it in wikifier part
        # and we don't know if the wikifier success or not here
        if cache_key and self.search_type != "wikifier":
            response = self.general_search_cache_manager.add_to_memcache(supplied_dataframe=self.supplied_dataframe,
                                                                         search_result_serialized=self.serialize(),
                                                                         augment_results=res,
                                                                         hash_key=cache_key
                                                                         )
            # save the augmented result's metadata if second augment is conducted
            if type(res) is not string and res != "failed":
                MetadataCache.save_metadata_from_dataset(res)
            if not response:
                self._logger.warning("Push augment results to results failed!")
            else:
                self._logger.info("Push augment results to memcache success!")

        return res

    def _augment(self, supplied_data, augment_columns=None, generate_metadata=True, return_format="ds",
                 augment_resource_id=AUGMENT_RESOURCE_ID):
        """
        Inner detail function for augment part
        """
        self._logger.debug("Start running augment function.")
        if type(return_format) is not str or return_format != "ds" and return_format != "df":
            raise ValueError("Unknown return format as" + str(return_format))

        if type(supplied_data) is d3m_Dataset:
            supplied_data_df = supplied_data[self._res_id]
        elif type(supplied_data) is d3m_DataFrame:
            supplied_data_df = supplied_data
        else:
            supplied_data_df = self.supplied_dataframe

        if supplied_data_df is None:
            raise ValueError("Can't find supplied data!")

        download_result = self.download(supplied_data=supplied_data_df, generate_metadata=False, return_format="df")
        download_result = download_result.drop(columns=['joining_pairs'])

        column_names_to_join = None
        r1_paired = set()
        i = 0

        df_dict = dict()
        start = time.time()
        columns_new = None
        for r1, r2 in self.pairs:
            i += 1
            r1_int = int(r1)
            if r1_int in r1_paired:
                continue
            r1_paired.add(r1_int)
            left_res = supplied_data_df.loc[r1_int]
            right_res = download_result.loc[int(r2)]
            if column_names_to_join is None:
                column_names_to_join = right_res.index.difference(left_res.index)
                if self.search_type == "general":
                    # only for general search condition, we should remove the target join columns
                    right_join_column_name = self.search_result['variableName']['value']
                    if right_join_column_name in column_names_to_join:
                        column_names_to_join = column_names_to_join.drop(right_join_column_name)
                # if specified augment columns given, only append these columns
                if augment_columns:
                    augment_columns_with_column_names = []
                    max_length = self.d3m_metadata.query((ALL_ELEMENTS,))['dimension']['length']
                    for each in augment_columns:
                        if each.column_index < max_length:
                            each_column_meta = self.d3m_metadata.query((ALL_ELEMENTS, each.column_index))
                            augment_columns_with_column_names.append(each_column_meta["name"])
                        else:
                            self._logger.error("Index out of range, will ignore: " + str(each.column_index))
                    column_names_to_join = column_names_to_join.intersection(augment_columns_with_column_names)

                columns_new = left_res.index.tolist()
                columns_new.extend(column_names_to_join.tolist())
            dcit_right = right_res[column_names_to_join].to_dict()
            dict_left = left_res.to_dict()
            dcit_right.update(dict_left)
            df_dict[i] = dcit_right

        df_joined = pd.DataFrame.from_dict(df_dict, "index")
        # add up the rows don't have pairs
        unpaired_rows = set(range(supplied_data_df.shape[0])) - r1_paired
        if len(unpaired_rows) > 0:
            unpaired_rows_list = [i for i in unpaired_rows]
            df_joined = df_joined.append(supplied_data_df.iloc[unpaired_rows_list, :], ignore_index=True)

        # ensure that the original dataframe columns are at the first left part
        if columns_new is not None:
            df_joined = df_joined[columns_new]
        else:
            self._logger.error("Attention! It seems augment do not add any extra columns!")

        # if search with wikidata, we should remove duplicate Q node column
        self._logger.info("Join finished, totally take " + str(time.time() - start) + " seconds.")

        if 'q_node' in df_joined.columns:
            df_joined = df_joined.drop(columns=['q_node'])

        if 'id' in df_joined.columns:
            df_joined = df_joined.drop(columns=['id'])

        # start adding column metadata for dataset
        if generate_metadata:
            return_result = self.metadata_manager.generate_metadata_for_augment_result(df_joined=df_joined,
                                                                                       return_format=return_format,
                                                                                       supplied_data=supplied_data,
                                                                                       augment_resource_id=augment_resource_id
                                                                                       )
        else:
            if return_format == "ds":
                self._logger.warning("It is useless to return a dataset without metadata!!!")
                return_df = d3m_DataFrame(df_joined, generate_metadata=False)
                resources = {augment_resource_id: return_df}
                return_result = d3m_Dataset(resources=resources, generate_metadata=False)
            else:
                return_result = d3m_DataFrame(df_joined)

        self._logger.debug("Augment finished")
        return return_result

    def score(self) -> float:
        return self.metadata_manager.score

    def id(self) -> str:
        return self.metadata_manager.id

    def get_metadata(self) -> DataMetadata:
        return self.d3m_metadata

    def set_join_pairs(self, join_pairs: typing.List["TabularJoinSpec"]) -> None:
        """
        manually set up the join pairs
        :param join_pairs: user specified TabularJoinSpec
        :return:
        """
        self.join_pairs = join_pairs

    def get_join_hints(self, left_df, right_df, left_df_src_id=None, right_src_id=None) -> typing.List["TabularJoinSpec"]:
        """
        Returns hints for joining supplied data with the data that can be downloaded using this search result.
        In the typical scenario, the hints are based on supplied data that was provided when search was called.

        The optional supplied_data argument enables the caller to request recomputation of join hints for specific data.

        :return: a list of join hints. Note that datamart is encouraged to return join hints but not required to do so.
        """
        self._logger.debug("Start getting join hints.")

        if self.search_type == "general":
            right_join_column_name = self.search_result['variableName']['value']
            left_columns = []
            right_columns = []

            for each in self.query_json['variables'].keys():
                left_index = left_df.columns.tolist().index(each)
                right_index = right_df.columns.tolist().index(right_join_column_name)
                left_index_column = DatasetColumn(resource_id=left_df_src_id, column_index=left_index)
                right_index_column = DatasetColumn(resource_id=right_src_id, column_index=right_index)
                left_columns.append([left_index_column])
                right_columns.append([right_index_column])

            results = TabularJoinSpec(left_columns=left_columns, right_columns=right_columns)
            self._logger.debug("Get join hints finished, the join hints are:")
            self._logger.debug(str(left_index) + ", " + str(right_index))
        else:
            raise ValueError("Unsupport type to get join hints with type" + self.search_type)
        return [results]

    def serialize(self) -> str:
        """
        Return a string format's json which contains all information needed for reproducing the augment
        :return:
        """
        result = dict()
        result['id'] = self.id()
        result['score'] = self.score()

        result['metadata'] = dict()
        result['metadata']['connection_url'] = self.connection_url
        result['metadata']['search_result'] = self.search_result
        result['metadata']['query_json'] = self.query_json
        result['metadata']['search_type'] = self.search_type
        augmentation = dict()
        augmentation['properties'] = "join"
        if self.search_type == "general":
            left_col_number = []
            right_col_number = None
            for each_key, each_value in literal_eval(self.search_result['extra_information']['value']).items():
                if 'name' in each_value.keys() and each_value['name'] == self.search_result['variableName']['value']:
                    right_col_number = int(each_key.split("_")[-1])
                    break
            augmentation['right_columns'] = [right_col_number]
            if self.supplied_dataframe is None:
                self._logger.error(
                    "Can't get supplied dataframe information, failed to find the left join column number")
            else:
                for each in self.query_json['variables'].keys():
                    left_col_number.append(self.supplied_dataframe.columns.tolist().index(each))
            augmentation['left_columns'] = left_col_number
        elif self.search_type == "wikidata":
            left_col_number = self.supplied_dataframe.columns.tolist().index(
                self.search_result['target_q_node_column_name'])
            augmentation['left_columns'] = [left_col_number]
            right_col_number = len(self.search_result['p_nodes_needed']) + 1
            augmentation['right_columns'] = [right_col_number]
        elif self.search_type == "vector":
            left_col_number = self.supplied_dataframe.columns.tolist().index(
                self.search_result['target_q_node_column_name'])
            augmentation['left_columns'] = [left_col_number]
            right_col_number = len(self.search_result['number_of_vectors']) # num of rows, not columns
            augmentation['right_columns'] = [right_col_number]
        result['augmentation'] = augmentation
        result['datamart_type'] = 'isi'
        result_str = json.dumps(result)

        return result_str

    @classmethod
    def deserialize(cls, serialize_result_str):
        serialize_result = json.loads(serialize_result_str)
        if "datamart_type" not in serialize_result or serialize_result["datamart_type"] != "isi":
            raise ValueError("False datamart type found")
        supplied_data = None  # serialize_result['metadata']['supplied_data']
        search_result = serialize_result['metadata']['search_result']
        query_json = serialize_result['metadata']['query_json']
        search_type = serialize_result['metadata']['search_type']
        return DatamartSearchResult(search_result, supplied_data, query_json, search_type)

    # def __getstate__(self) -> typing.Dict:
    #     """
    #     This method is used by the pickler as the state of object.
    #     The object can be recovered through this state uniquely.
    #     Returns:
    #         state: Dict
    #             dictionary of important attributes of the object
    #     """
    #     state = dict()
    #     state["search_result"] = self.__dict__["search_result"]
    #     state["query_json"] = self.__dict__["query_json"]
    #     state["search_type"] = self.__dict__["search_type"]
    #
    #     return state
    #
    # def __setstate__(self, state: typing.Dict) -> None:
    #     """
    #     This method is used for unpickling the object. It takes a dictionary
    #     of saved state of object and restores the object to that state.
    #     Args:
    #         state: typing.Dict
    #             dictionary of the objects picklable state
    #     Returns:
    #     """
    #     self = self.__init__(search_result=state['search_result'],
    #                          supplied_data=None,
    #                          query_json=state['query_json'],
    #                          search_type=state['search_type'])


class TabularJoinSpec(AugmentSpec):
    """
    A join spec specifies a possible way to join a left dataset with a right dataset. The spec assumes that it may
    be necessary to use several columns in each datasets to produce a key or fingerprint that is useful for joining
    datasets. The spec consists of two lists of column identifiers or names (left_columns, left_column_names and
    right_columns, right_column_names).

    In the simplest case, both left and right are singleton lists, and the expectation is that an appropriate
    matching function exists to adequately join the datasets. In some cases equality may be an appropriate matching
    function, and in some cases fuzz matching is required. The join spec does not specify the matching function.

    In more complex cases, one or both left and right lists contain several elements. For example, the left list
    may contain columns for "city", "state" and "country" and the right dataset contains an "address" column. The join
    spec pairs up ["city", "state", "country"] with ["address"], but does not specify how the matching should be done
    e.g., combine the city/state/country columns into a single column, or split the address into several columns.
    """

    def __init__(self, left_columns: typing.List[typing.List[DatasetColumn]],
                 right_columns: typing.List[typing.List[DatasetColumn]],
                 left_resource_id: str = None, right_resource_id: str = None) -> None:

        self.left_resource_id = left_resource_id
        self.right_resource_id = right_resource_id
        self.left_columns = left_columns
        self.right_columns = right_columns
        if len(self.left_columns) != len(self.right_columns):
            shorter_len = min(len(self.right_columns), len(self.left_columns))
            self.left_columns = self.left_columns[:shorter_len]
            self.right_columns = self.right_columns[:shorter_len]
            print("The join spec length on left and right are different! Part of them will be ignored")

        # we can have list of the joining column pairs
        # each list inside left_columns/right_columns is a candidate joining column for that dataFrame
        # each candidate joining column can also have multiple columns

    def get_column_number_pairs(self):
        """
            A simple function used to get the pairs of column numbers only
            For example, it will return a join pair like ([1,2], [1])
        """
        all_pairs = []
        for each in zip(self.left_columns, self.right_columns):
            left = []
            right = []
            for each_left_col in each[0]:
                left.append(each_left_col.column_index)
            for each_right_col in each[1]:
                right.append(each_right_col.column_index)
            all_pairs.append((left, right))
        return all_pairs


class VariableConstraint(object):
    """
    Abstract class for all variable constraints.
    """

    def __init__(self, key: str, values: str):
        self.key = key
        self.values = values


class TemporalGranularity(utils.Enum):
    YEAR = 1
    MONTH = 2
    DAY = 3
    HOUR = 4
    SECOND = 5


class DatamartQuery:
    """
    A Datamart query consists of two parts:

    * A list of keywords.

    * A list of required variables. A required variable specifies that a matching dataset must contain a variable
      satisfying the constraints provided in the query. When multiple required variables are given, the matching
      dataset should contain variables that match each of the variable constraints.

    The matching is fuzzy. For example, when a user specifies a required variable spec using named entities, the
    expectation is that a matching dataset contains information about the given named entities. However, due to name,
    spelling, and other differences it is possible that the matching dataset does not contain information about all
    the specified entities.

    In general, Datamart will do a best effort to satisfy the constraints, but may return datasets that only partially
    satisfy the constraints.
    """

    def __init__(self, keywords: typing.List[str] = list(), variables: typing.List['VariableConstraint'] = list(),
                 search_type: str = "general", keywords_search: typing.List[str] = list(), title_search: str = "",
                 variables_search: dict() = dict()) -> None:
        self.search_type = search_type
        self.keywords = keywords
        self.variables = variables
        self.keywords_search = keywords_search
        self.title_search = title_search
        self.variables_search = variables_search
