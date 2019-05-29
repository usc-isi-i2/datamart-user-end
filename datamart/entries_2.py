
import datetime
import typing
import pandas as pd
import copy
import random
import frozendict
import collections
import typing
from d3m.container import DataFrame as d3m_DataFrame
from d3m.container import Dataset as d3m_Dataset
import d3m.metadata.base as metadata_base
# from datamart.dataset import Dataset
from datamart.utilities.utils import PRODUCTION_ES_INDEX, SEARCH_URL
from datamart.es_managers.json_query_manager import JSONQueryManager
from datamart.new_query.augment import Augment
# old Augment 
# from datamart.augment import Augment
# from datamart.data_loader import DataLoader
from d3m.base import utils as d3m_utils
from datamart.utilities.utils import Utils
# from datamart.joiners.join_result import JoinResult
# from datamart.joiners.joiner_base import JoinerType
# from itertools import chain
from datamart.joiners.rltk_joiner import RLTKJoiner
from SPARQLWrapper import SPARQLWrapper, JSON, POST, URLENCODED
from d3m.metadata.base import DataMetadata, ALL_ELEMENTS
from datamart.joiners.rltk_joiner import RLTKJoiner_new
from wikifier import config
import wikifier
# import requests
import traceback
import logging
import datetime
import enum

from d3m import container
import d3m.metadata.base as metadata_base
from d3m import utils

__all__ = ('DatamartQueryCursor', 'Datamart', 'DatasetColumn', 'DatamartSearchResult', 'AugmentSpec',
           'TabularJoinSpec', 'UnionSpec', 'TemporalGranularity', 'GeospatialGranularity', 'ColumnRelationship', 'DatamartQuery',
           'VariableConstraint', 'NamedEntityVariable', 'TemporalVariable', 'GeospatialVariable', 'TabularVariable')

Q_NODE_SEMANTIC_TYPE = "http://wikidata.org/qnode"
DEFAULT_URL = "https://isi-datamart.edu"
__ALL__ = ('D3MDatamart', 'DatamartSearchResult', 'D3MJoinSpec')
DatamartSearchResult = typing.TypeVar('DatamartSearchResult', bound='DatamartSearchResult')
D3MJoinSpec = typing.TypeVar('D3MJoinSpec', bound='D3MJoinSpec')
DatamartQuery = typing.TypeVar('DatamartQuery', bound='DatamartQuery')
MAX_ENTITIES_LENGTH = 200
CONTAINER_SCHEMA_VERSION = 'https://metadata.datadrivendiscovery.org/schemas/v0/container.json'
P_NODE_IGNORE_LIST = {"P1549"}
SPECIAL_REQUEST_FOR_P_NODE = {"P1813": "FILTER(strlen(str(?P1813)) = 2)"}
AUGMENT_RESOURCE_ID = "augmentData"
WIKIDATA_QUERY_SERVER = config.endpoint_query_main


class DatamartQueryCursor(object):
    """
    Cursor to iterate through Datamarts search results.
    """
    def __init__(self, search_query, supplied_data, need_run_wikifier=False):
        self.search_query = search_query
        self.current_searching_query_index = 0
        self.supplied_data = supplied_data
        self.remained_part = None
        self.need_run_wikifier = need_run_wikifier

    def get_next_page(self, *, limit: typing.Optional[int] = 20, timeout: int = None) -> typing.Optional[typing.Sequence['DatamartSearchResult']]:
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

        # if need to run wikifier, run it before any search
        if self.current_searching_query_index == 0 and self.need_run_wikifier:
            self._run_wikifier()

        # if already remianed enough part
        current_result = self.remained_part or []
        if len(current_result) > limit:
            self.remained_part = current_result[limit:]
            current_result = current_result[:limit]
            return current_result

        # start searching
        while self.current_searching_query_index < len(self.search_query) and len(current_result) < limit:
            if self.search_query[self.current_searching_query_index].search_type == "wikidata":
                search_res = self._search_wikidata(query=None, supplied_data=self.supplied_data)
            elif self.search_query[self.current_searching_query_index].search_type == "datamart":
                search_res = self._search_datamart()
            
            self.current_searching_query_index += 1
            current_result.extend(search_res)
            

        if len(current_result) == 0:
            return None
        else:
            if len(current_result) > limit:
                self.remained_part = current_result[limit:]
                current_result = current_result[:limit]
            return current_result

    def run_wikifier(self):
        from dsbox.datapreprocessing.cleaner.wikifier import WikifierHyperparams ,Wikifier
        wikifier_hyperparams = WikifierHyperparams.defaults()
        # wikifier_hyperparams = wikifier_hyperparams.replace({"use_columns":(1,)})
        wikifier_primitive = Wikifier(hyperparams = wikifier_hyperparams)
        self.supplied_data = wikifier_primitive.produce(inputs = self.supplied_data).value
        self.need_run_wikifier = False


    def _search_wikidata(self, query, supplied_data: typing.Union[d3m_DataFrame, d3m_Dataset]=None, timeout=None,
                         search_threshold=0.5) -> typing.List["DatamartSearchResult"]:
        """
        The search function used for wikidata search
        :param query: JSON object describing the query.
        :param supplied_data: the data you are trying to augment.
        :param timeout: allowed time spent on searching
        :param limit: the limitation on the return amount of DatamartSearchResult
        :param search_threshold: the minimum appeared times of the properties
        :return: list of search results of DatamartSearchResult
        """
        wikidata_results = []
        try:
            q_nodes_columns = []
            if type(supplied_data) is d3m_Dataset:
                res_id, supplied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None)
                selector_base_type = "ds"
            else:
                supplied_dataframe = supplied_data
                selector_base_type = "df"

            # check whether Qnode is given in the inputs, if given, use this to wikidata and search
            required_variables_names = None
            metadata_input = supplied_data.metadata

            if query is not None and 'required_variables' in query:
                required_variables_names = []
                for each in query['required_variables']:
                    required_variables_names.extend(each['names'])
            for i in range(supplied_dataframe.shape[1]):
                if selector_base_type == "ds":
                    metadata_selector = (res_id, metadata_base.ALL_ELEMENTS, i)
                else:
                    metadata_selector = (metadata_base.ALL_ELEMENTS, i)
                if Q_NODE_SEMANTIC_TYPE in metadata_input.query(metadata_selector)["semantic_types"]:
                    # if no required variables given, attach any Q nodes found
                    if required_variables_names is None:
                        q_nodes_columns.append(i)
                    # otherwise this column has to be inside required_variables
                    else:
                        if supplied_dataframe.columns[i] in required_variables_names:
                            q_nodes_columns.append(i)

            if len(q_nodes_columns) == 0:
                print("No wikidata Q nodes detected on corresponding required_variables! Will skip wikidata search part")
                return wikidata_results
            else:

                print("Wikidata Q nodes inputs detected! Will search with it.")
                print("Totally " + str(len(q_nodes_columns)) + " Q nodes columns detected!")

                # do a wikidata search for each Q nodes column
                for each_column in q_nodes_columns:
                    q_nodes_list = supplied_dataframe.iloc[:, each_column].tolist()
                    p_count = collections.defaultdict(int)
                    p_nodes_needed = []
                    # temporary block
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
                    # TODO: temporary change here, may change back in the future
                    # Q node format (wd:Q23)(wd: Q42)
                    q_node_query_part = ""
                    unique_qnodes = set(q_nodes_list)
                    for each in unique_qnodes:
                        if len(each) > 0:
                            q_node_query_part += "(wd:" + each + ")"
                    sparql_query = "select distinct ?item ?property where \n{\n  VALUES (?item) {" + q_node_query_part \
                                   + "  }\n  ?item ?property ?value .\n  ?wd_property wikibase:directClaim ?property ." \
                                   + "  values ( ?type ) \n  {\n    ( wikibase:Quantity )\n" \
                                   + "    ( wikibase:Time )\n    ( wikibase:Monolingualtext )\n  }" \
                                   + "  ?wd_property wikibase:propertyType ?type .\n}\norder by ?item ?property "

                    try:
                        sparql = SPARQLWrapper(WIKIDATA_QUERY_SERVER)
                        sparql.setQuery(sparql_query)
                        sparql.setReturnFormat(JSON)
                        sparql.setMethod(POST)
                        sparql.setRequestMethod(URLENCODED)
                        results = sparql.query().convert()['results']['bindings']
                    except:
                        print("Query failed!")
                        traceback.print_exc()
                        continue

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
            return wikidata_results

        except:
            print("Searching with wiki data failed")
            traceback.print_exc()
        finally:
            return wikidata_results

    def _search_datamart(self):
        pass

class Datamart(object):
    """
    All Datamarts must implement this abstract class.
    """

    def __init__(self, connection_url: str) -> None:
        self.connection_url = connection_url
        self._logger = logging.getLogger(__name__)
        # query_server = "http://dsbox02.isi.edu:9999/blazegraph/namespace/datamart3/sparql"  # config.endpoint_query_main
        self.augmenter = Augment(endpoint=self.connection_url)

    def set_test_mode(self) -> None:
        query_server = config.endpoint_query_test
        self.augmenter = Augment(endpoint=query_server)

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
        print("Not implemented yet")
        pass

    def search_with_data(self, query: 'DatamartQuery', supplied_data: container.Dataset) -> DatamartQueryCursor:
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
        search_queries = [DatamartQuery(search_type="wikidata")]
        if type(supplied_data) is d3m_Dataset:
            res_id, supplied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None)
        else:
            supplied_dataframe = supplied_data

        if query is None:
            # if not query given, try to find the Text columns from given dataframe and use it to find some candidates
            can_query_columns = []
            for each in range(len(supplied_dataframe.columns)):
                if type(supplied_data) is d3m_Dataset:
                    selector = (res_id, ALL_ELEMENTS, each)
                else:
                    selector = (ALL_ELEMENTS, each)
                each_column_meta = supplied_data.metadata.query(selector)
                if 'http://schema.org/Text' in each_column_meta["semantic_types"]:
                    # or "https://metadata.datadrivendiscovery.org/types/CategoricalData" in each_column_meta["semantic_types"]:
                    can_query_columns.append(each)


            if len(can_query_columns) == 0:
                self._logger.warning("No columns can be augment with datamart!")
            
            for each_column in can_query_columns:
                tabular_variable = TabularVariable(columns=[each_column], relationship=None)
                each_search_query = self._generate_datamart_query_from_data(query=None, supplied_data=supplied_data, data_constraints=tabular_variable)
                search_queries.append(each_search_query)

            return DatamartQueryCursor(search_query=search_queries, supplied_data=supplied_data)

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

        # put the enetities of all given columns from "data_constraints" into the query's variable part and run the query

        search_query = self._generate_datamart_query_from_data(query=None, supplied_data=supplied_data, data_constraints=data_constraints)
        return DatamartQueryCursor(search_query=[search_query], supplied_data=supplied_data)

    def _generate_datamart_query_from_data(self, query: 'DatamartQuery', supplied_data: container.Dataset,
                                 data_constraints: typing.List['TabularVariable']):
        all_query_variables = ""
        for each_column in data_constraints.columns:
            column_values = supplied_dataframe.iloc[:, each_column]
            query_column_entities = list(set(column_values.tolist()))

            if len(query_column_entities) > MAX_ENTITIES_LENGTH:
                query_column_entities = random.sample(query_column_entities, MAX_ENTITIES_LENGTH)

            for i in range(len(query_column_entities)):
                query_column_entities[i] = str(query_column_entities[i])

            query_column_entities = " ".join(query_column_entities)

        all_query_variables += query_column_entities + " "
        search_query = DatamartQuery(variables=query_column_entities)

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

    Attributes
    ----------
    join_hints: typing.List[D3MAugmentSpec]
        Hints for joining supplied data with datamart data

    """
    def __init__(self, search_result, supplied_data, query_json, search_type):
        self._logger = logging.getLogger(__name__)
        self.search_result = search_result
        if "_score" in self.search_result:
            self.score = self.search_result["_score"]
        if "_source" in self.search_result:
            self.metadata = self.search_result["_source"]
        self.supplied_data = supplied_data

        if type(supplied_data) is d3m_Dataset:
            self.res_id, self.supplied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None)
            self.selector_base_type = "ds"
        elif type(supplied_data) is d3m_DataFrame:
            self.supplied_dataframe = supplied_data
            self.selector_base_type = "df"

        self.query_json = query_json
        self.search_type = search_type
        self.pairs = None
        self._res_id = None  # only used for input is Dataset
        self.join_pairs = None

    def display(self) -> pd.DataFrame:
        """
        function used to see what found inside this search result class in a human vision
        :return: a pandas DataFrame
        """
        if self.search_type == "wikidata":
            column_names = []
            for each in self.search_result["p_nodes_needed"]:
                each_name = self._get_node_name(each)
                column_names.append(each_name)
            column_names = ", ".join(column_names)
            required_variable = []
            required_variable.append(self.search_result["target_q_node_column_name"])
            result = pd.DataFrame({"title": "wikidata search result for " \
                                           + self.search_result["target_q_node_column_name"], \
                                   "columns": column_names, "join columns": required_variable}, index=[0])

        elif self.search_type == "general":
            title = self.search_result['_source']['title']
            column_names = []
            required_variable = []
            for each in self.query_json['required_variables']:
                required_variable.append(each['names'])

            for each in self.search_result['_source']['variables']:
                each_name = each['name']
                column_names.append(each_name)
            column_names = ", ".join(column_names)
            result = pd.DataFrame({"title": title, "columns": column_names, "join columns": required_variable}, index=[0])

        return result

    def download(self, supplied_data: typing.Union[d3m_Dataset, d3m_DataFrame], generate_metadata=True, return_format="ds") \
            -> typing.Union[d3m_Dataset, d3m_DataFrame]:
        """
        download the dataset or dataFrame (depending on the input type) and corresponding metadata information of
        search result everytime call download, the DataFrame will have the exact same columns in same order
        """
        if self.search_type=="general":
            return_df = self.download_general(supplied_data, generate_metadata, return_format)
        elif self.search_type=="wikidata":
            return_df = self.download_wikidata(supplied_data, generate_metadata, return_format)

        return return_df

    def download_general(self, supplied_data: typing.Union[d3m_Dataset, d3m_DataFrame]=None, generate_metadata=True,
                         return_format="ds", augment_resource_id = AUGMENT_RESOURCE_ID) -> typing.Union[d3m_Dataset, d3m_DataFrame]:
        """
        Specified download function for general datamart Datasets
        :param supplied_data: given supplied data
        :param generate_metadata: whether need to genreate the metadata or not
        :return: a dataset or a dataframe depending on the input
        """
        if type(supplied_data) is d3m_Dataset:
            self._res_id, self.supplied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None)
        else:
            self.supplied_dataframe = supplied_data

        if self.join_pairs is None:
            candidate_join_column_pairs = self.get_join_hints()
        else:
            candidate_join_column_pairs = self.join_pairs

        if len(candidate_join_column_pairs) > 1:
            print("[WARN]: multiple joining column pairs found")
        join_pairs_result = []
        candidate_join_column_scores = []

        # start finding pairs
        if supplied_data is None:
            supplied_data = self.supplied_dataframe
        left_df = copy.deepcopy(self.supplied_dataframe)
        right_metadata = self.search_result['_source']
        right_df = Utils.materialize(metadata=self.metadata)
        left_metadata = Utils.generate_metadata_from_dataframe(data=left_df, original_meta=None)

        # generate the pairs for each join_column_pairs
        for each_pair in candidate_join_column_pairs:
            left_columns = each_pair.left_columns
            right_columns = each_pair.right_columns
            try:
                # Only profile the joining columns, otherwise it will be too slow:
                left_metadata = Utils.calculate_dsbox_features(data=left_df, metadata=left_metadata,
                                                               selected_columns=set(left_columns))

                right_metadata = Utils.calculate_dsbox_features(data=right_df, metadata=right_metadata,
                                                                selected_columns=set(right_columns))
                # update with implicit_variable on the user supplied dataset
                if left_metadata.get('implicit_variables'):
                    Utils.append_columns_for_implicit_variables_and_add_meta(left_metadata, left_df)

                print(" - start getting pairs for", each_pair.to_str_format())

                result, self.pairs = RLTKJoiner.find_pair(left_df=left_df, right_df=right_df,
                                                          left_columns=[left_columns], right_columns=[right_columns],
                                                          left_metadata=left_metadata, right_metadata=right_metadata)

                join_pairs_result.append(result)
                # TODO: figure out some way to compute the joining quality
                candidate_join_column_scores.append(100)
            except:
                print("failed when getting pairs for", each_pair)
                traceback.print_exc()

        # choose the best joining results
        all_results = []
        for i in range(len(join_pairs_result)):
            each_result = (candidate_join_column_pairs[i], candidate_join_column_scores[i], join_pairs_result[i])
            all_results.append(each_result)

        all_results.sort(key=lambda x: x[1], reverse=True)
        if len(all_results) == 0:
            raise ValueError("[ERROR] Failed to get pairs!")

        if return_format == "ds":
            return_df = d3m_DataFrame(all_results[0][2], generate_metadata=False)
            resources = {augment_resource_id: return_df}
            return_result = d3m_Dataset(resources=resources, generate_metadata=False)
            if generate_metadata:
                metadata_shape_part_dict = self._generate_metadata_shape_part(value=return_result, selector=())
                for each_selector, each_metadata in metadata_shape_part_dict.items():
                    return_result.metadata = return_result.metadata.update(selector=each_selector, metadata=each_metadata)
                return_result.metadata = self._generate_metadata_column_part_for_general(return_result.metadata, return_format, augment_resource_id)

        elif return_format == "df":
            return_result = d3m_DataFrame(all_results[0][2], generate_metadata=False)
            if generate_metadata:
                metadata_shape_part_dict = self._generate_metadata_shape_part(value=return_result, selector=())
                for each_selector, each_metadata in metadata_shape_part_dict.items():
                    return_result.metadata = return_result.metadata.update(selector=each_selector, metadata=each_metadata)
                return_result.metadata = self._generate_metadata_column_part_for_general(return_result.metadata, return_format, augment_resource_id=None)

        else:
            raise ValueError("Invalid return format was given")

        return return_result

    def _generate_metadata_shape_part(self, value, selector) -> dict:
        """
        recursively generate all metadata for shape part, return a dict
        :param value:
        :param selector:
        :return:
        """
        generated_metadata: dict = {}
        generated_metadata['schema'] = CONTAINER_SCHEMA_VERSION
        if isinstance(value, d3m_Dataset):  # type: ignore
            generated_metadata['dimension'] = {
                'name': 'resources',
                'semantic_types': ['https://metadata.datadrivendiscovery.org/types/DatasetResource'],
                'length': len(value),
            }

            metadata_dict = collections.OrderedDict([(selector, generated_metadata)])

            for k, v in value.items():
                metadata_dict.update(self._generate_metadata_shape_part(v, selector + (k,)))

            # It is unlikely that metadata is equal across dataset resources, so we do not try to compact metadata here.

            return metadata_dict

        if isinstance(value, d3m_DataFrame):  # type: ignore
            generated_metadata['semantic_types'] = ['https://metadata.datadrivendiscovery.org/types/Table']

            generated_metadata['dimension'] = {
                'name': 'rows',
                'semantic_types': ['https://metadata.datadrivendiscovery.org/types/TabularRow'],
                'length': value.shape[0],
            }

            metadata_dict = collections.OrderedDict([(selector, generated_metadata)])

            # Reusing the variable for next dimension.
            generated_metadata = {
                'dimension': {
                    'name': 'columns',
                    'semantic_types': ['https://metadata.datadrivendiscovery.org/types/TabularColumn'],
                    'length': value.shape[1],
                },
            }

            selector_all_rows = selector + (ALL_ELEMENTS,)
            metadata_dict[selector_all_rows] = generated_metadata
            return metadata_dict

    def _generate_metadata_column_part_for_general(self, metadata_return, return_format, augment_resource_id) -> DataMetadata:
        """
        Inner function used to generate metadata for general search
        """
        # part for adding the whole dataset/ dataframe's metadata


        # part for adding each column's metadata
        for i, each_metadata in enumerate(self.metadata['variables']):
            if return_format == "ds":
                metadata_selector = (augment_resource_id, ALL_ELEMENTS, i)
            elif return_format == "df":
                metadata_selector = (ALL_ELEMENTS, i)
            structural_type = each_metadata["description"].split("dtype: ")[-1]
            if "int" in structural_type:
                structural_type = int
            elif "float" in structural_type:
                structural_type = float
            else:
                structural_type = str
            metadata_each_column = {"name": each_metadata["name"], "structural_type": structural_type,
                                    'semantic_types': ('https://metadata.datadrivendiscovery.org/types/Attribute',)}
            metadata_return = metadata_return.update(metadata=metadata_each_column, selector=metadata_selector)

        if return_format == "ds":
            metadata_selector = (augment_resource_id, ALL_ELEMENTS, i + 1)
        elif return_format == "df":
            metadata_selector = (ALL_ELEMENTS, i + 1)
        metadata_joining_pairs = {"name": "joining_pairs", "structural_type": typing.List[int],
                                  'semantic_types': ("http://schema.org/Integer",)}
        metadata_return = metadata_return.update(metadata=metadata_joining_pairs, selector=metadata_selector)

        return metadata_return

    def download_wikidata(self, supplied_data: typing.Union[d3m_Dataset, d3m_DataFrame], generate_metadata=True, return_format="ds",augment_resource_id=AUGMENT_RESOURCE_ID) -> typing.Union[d3m_Dataset, d3m_DataFrame]:
        """
        :param supplied_data: input DataFrame
        :param generate_metadata: control whether to automatically generate metadata of the return DataFrame or not
        :return: return_df: the materialized wikidata d3m_DataFrame,
                            with corresponding pairing information to original_data at last column
        """
        # prepare the query
        p_nodes_needed = self.search_result["p_nodes_needed"]
        target_q_node_column_name = self.search_result["target_q_node_column_name"]
        if type(supplied_data) is d3m_DataFrame:
            self.supplied_dataframe = copy.deepcopy(supplied_data)
        elif type(supplied_data) is d3m_Dataset:
            self._res_id, supplied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data,
                                                                                  resource_id=None)
            self.supplied_dataframe = copy.deepcopy(supplied_dataframe)

        q_node_column_number = self.supplied_dataframe.columns.tolist().index(target_q_node_column_name)
        q_nodes_list = set(self.supplied_dataframe.iloc[:, q_node_column_number].tolist())
        q_nodes_query = ""
        p_nodes_query_part = ""
        p_nodes_optional_part = ""
        special_request_part = ""
        # q_nodes_list = q_nodes_list[:30]
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
                       "WHERE \n{\n  VALUES (?q) { \n " + q_nodes_query + "}\n" + \
                       p_nodes_optional_part + special_request_part + "}\n"
        import pdb
        pdb.set_trace()
        return_df = d3m_DataFrame()
        try:
            sparql = SPARQLWrapper(WIKIDATA_QUERY_SERVER)
            sparql.setQuery(sparql_query)
            sparql.setReturnFormat(JSON)
            sparql.setMethod(POST)

            sparql.setRequestMethod(URLENCODED)
            results = sparql.query().convert()
        except:
            print("Getting query of wiki data failed!")
            return return_df

        semantic_types_dict = {
            "q_node": ("http://schema.org/Text", 'https://metadata.datadrivendiscovery.org/types/PrimaryKey')}

        for result in results["results"]["bindings"]:
            each_result = {}
            q_node_name = result.pop("q")["value"].split("/")[-1]
            each_result["q_node"] = q_node_name
            for p_name, p_val in result.items():
                each_result[p_name] = p_val["value"]
                # only do this part if generate_metadata is required
                if p_name not in semantic_types_dict:
                    if "datatype" in p_val.keys():
                        semantic_types_dict[p_name] = (
                            self._get_semantic_type(p_val["datatype"]),
                            'https://metadata.datadrivendiscovery.org/types/Attribute')
                    else:
                        semantic_types_dict[p_name] = (
                            "http://schema.org/Text", 'https://metadata.datadrivendiscovery.org/types/Attribute')

            return_df = return_df.append(each_result, ignore_index=True)

        p_name_dict = {"q_node": "q_node"}
        for each in return_df.columns.tolist():
            if each.lower().startswith("p") or each.lower().startswith("c"):
                p_name_dict[each] = self._get_node_name(each)

        # use rltk joiner to find the joining pairs
        joiner = RLTKJoiner_new()
        joiner.set_join_target_column_names((self.supplied_dataframe.columns[q_node_column_number], "q_node"))
        result, self.pairs = joiner.find_pair(left_df=self.supplied_dataframe, right_df=return_df)

        # if this condition is true, it means "id" column was added which should not be here
        if return_df.shape[1] == len(p_name_dict) + 2 and "id" in return_df.columns:
            return_df = return_df.drop(columns=["id"])

        metadata_new = DataMetadata()
        self.metadata = {}

        # add remained attributes metadata
        for each_column in range(0, return_df.shape[1] - 1):
            current_column_name = p_name_dict[return_df.columns[each_column]]
            metadata_selector = (ALL_ELEMENTS, each_column)
            # here we do not modify the original data, we just add an extra "expected_semantic_types" to metadata
            metadata_each_column = {"name": current_column_name, "structural_type": str,
                                    'semantic_types': semantic_types_dict[return_df.columns[each_column]]}
            self.metadata[current_column_name] = metadata_each_column
            if generate_metadata:
                metadata_new = metadata_new.update(metadata=metadata_each_column, selector=metadata_selector)

        # special for joining_pairs column
        metadata_selector = (ALL_ELEMENTS, return_df.shape[1])
        metadata_joining_pairs = {"name": "joining_pairs", "structural_type": typing.List[int],
                                  'semantic_types': ("http://schema.org/Integer",)}
        if generate_metadata:
            metadata_new = metadata_new.update(metadata=metadata_joining_pairs, selector=metadata_selector)

        # start adding shape metadata for dataset
        if return_format == "ds":
            return_df = d3m_DataFrame(return_df, generate_metadata=False)
            return_df = return_df.rename(columns=p_name_dict)
            resources = {augment_resource_id: return_df}
            return_result = d3m_Dataset(resources=resources, generate_metadata=False)
            if generate_metadata:
                return_result.metadata = metadata_new
                metadata_shape_part_dict = self._generate_metadata_shape_part(value=return_result, selector=())
                for each_selector, each_metadata in metadata_shape_part_dict.items():
                    return_result.metadata = return_result.metadata.update(selector=each_selector,
                                                                           metadata=each_metadata)
            # update column names to be property names instead of number

        elif return_format == "df":
            return_result = d3m_DataFrame(return_df, generate_metadata=False)
            return_result = return_result.rename(columns=p_name_dict)
            if generate_metadata:
                return_result.metadata = metadata_new
                metadata_shape_part_dict = self._generate_metadata_shape_part(value=return_result, selector=())
                for each_selector, each_metadata in metadata_shape_part_dict.items():
                    return_result.metadata = return_result.metadata.update(selector=each_selector,
                                                                           metadata=each_metadata)
        return return_result

    def _get_node_name(self, node_code):
        """
        Inner function used to get the properties(P nodes) names with given P node
        :param node_code: a str indicate the P node (e.g. "P123")
        :return: a str indicate the P node label (e.g. "inception")
        """
        sparql_query = "SELECT DISTINCT ?x WHERE \n { \n" + \
                       "wd:" + node_code + " rdfs:label ?x .\n FILTER(LANG(?x) = 'en') \n} "
        try:
            sparql = SPARQLWrapper(WIKIDATA_QUERY_SERVER)
            sparql.setQuery(sparql_query)
            sparql.setReturnFormat(JSON)
            sparql.setMethod(POST)
            sparql.setRequestMethod(URLENCODED)
            results = sparql.query().convert()
            return results['results']['bindings'][0]['x']['value']
        except:
            print("Getting name of node " + node_code + " failed!")
            return node_code

    def _get_semantic_type(self, datatype: str):
        """
        Inner function used to transfer the wikidata semantic type to D3M semantic type
        :param datatype: a str indicate the semantic type adapted from wikidata
        :return: a str indicate the semantic type for D3M
        """
        special_type_dict = {"http://www.w3.org/2001/XMLSchema#dateTime": "http://schema.org/DateTime",
                             "http://www.w3.org/2001/XMLSchema#decimal": "http://schema.org/Float",
                             "http://www.opengis.net/ont/geosparql#wktLiteral": "https://metadata.datadrivendiscovery.org/types/Location"}
        default_type = "http://schema.org/Text"
        if datatype in special_type_dict:
            return special_type_dict[datatype]
        else:
            print("not seen type : ", datatype)
            return default_type

    def augment(self, supplied_data, generate_metadata=True, augment_resource_id=AUGMENT_RESOURCE_ID):
        """
        download and join using the D3mJoinSpec from get_join_hints()
        """
        if type(supplied_data) is d3m_DataFrame:
            return self._augment(supplied_data=supplied_data, generate_metadata=generate_metadata, return_format="df", augment_resource_id=augment_resource_id)
        elif type(supplied_data) is d3m_Dataset:
            self._res_id, self.supplied_data = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None, has_hyperparameter=False)
            res = self._augment(supplied_data=supplied_data, generate_metadata=generate_metadata, return_format="ds", augment_resource_id=augment_resource_id)
            return res

    def _augment(self, supplied_data, generate_metadata=True, return_format="ds", augment_resource_id=AUGMENT_RESOURCE_ID):
        """
        download and join using the D3mJoinSpec from get_join_hints()
        """
        if type(supplied_data) is d3m_Dataset:
            supplied_data_df = supplied_data[self._res_id]
        else:
            supplied_data_df = supplied_data

        download_result = self.download(supplied_data=supplied_data_df, generate_metadata=False, return_format="df")
        download_result = download_result.drop(columns=['joining_pairs'])
        df_joined = pd.DataFrame()

        column_names_to_join = None
        for r1, r2 in self.pairs:
            left_res = supplied_data_df.loc[int(r1)]
            right_res = download_result.loc[int(r2)]
            if column_names_to_join is None:
                column_names_to_join = right_res.index.difference(left_res.index)
                matched_rows = right_res.index.intersection(left_res.index)
                columns_new = left_res.index.tolist()
                columns_new.extend(column_names_to_join.tolist())
            new = pd.concat([left_res, right_res[column_names_to_join]])
            df_joined = df_joined.append(new, ignore_index=True)
        # ensure that the original dataframe columns are at the first left part
        df_joined = df_joined[columns_new]
        # if search with wikidata, we can remove duplicate Q node column

        if self.search_type == "wikidata":
            df_joined = df_joined.drop(columns=['q_node'])

        if 'id' in df_joined.columns:
            df_joined = df_joined.drop(columns=['id'])

        if generate_metadata:
            columns_all = list(df_joined.columns)
            if 'd3mIndex' in df_joined.columns:
                oldindex = columns_all.index('d3mIndex')
                columns_all.insert(0, columns_all.pop(oldindex))
            else:
                self._logger.warning("No d3mIndex column found after data-mart augment!!!")
            df_joined = df_joined[columns_all]

        # start adding column metadata for dataset
        if generate_metadata:
            metadata_dict_left = {}
            metadata_dict_right = {}
            if self.search_type == "general":
                for each in self.metadata['variables']:
                    description = each['description']
                    dtype = description.split("dtype: ")[-1]
                    if "float" in dtype:
                        semantic_types = (
                            "http://schema.org/Float",
                            "https://metadata.datadrivendiscovery.org/types/Attribute"
                        )
                    elif "int" in dtype:
                        semantic_types = (
                            "http://schema.org/Integer",
                            "https://metadata.datadrivendiscovery.org/types/Attribute"
                        )
                    else:
                        semantic_types = (
                            "https://metadata.datadrivendiscovery.org/types/CategoricalData",
                            "https://metadata.datadrivendiscovery.org/types/Attribute"
                        )
                    each_meta = {
                        "name": each['name'],
                        "structural_type": str,
                        "semantic_types": semantic_types,
                        "description": description
                    }
                    metadata_dict_right[each['name']] = frozendict.FrozenOrderedDict(each_meta)
            else:
                metadata_dict_right = self.metadata

            if return_format == "df":
                left_df_column_legth = supplied_data.metadata.query((metadata_base.ALL_ELEMENTS,))['dimension'][
                    'length']
            elif return_format == "ds":
                left_df_column_legth = supplied_data.metadata.query((self._res_id, metadata_base.ALL_ELEMENTS,))['dimension']['length']

            # add the original metadata
            for i in range(left_df_column_legth):
                if return_format == "df":
                    each_selector = (ALL_ELEMENTS, i)
                elif return_format == "ds":
                    each_selector = (self._res_id, ALL_ELEMENTS, i)
                each_column_meta = supplied_data.metadata.query(each_selector)
                metadata_dict_left[each_column_meta['name']] = each_column_meta

            metadata_new = metadata_base.DataMetadata()
            metadata_old = copy.copy(supplied_data.metadata)

            new_column_names_list = list(df_joined.columns)
            # update each column's metadata
            for i, current_column_name in enumerate(new_column_names_list):
                if return_format == "df":
                    each_selector = (metadata_base.ALL_ELEMENTS, i)
                elif return_format == "ds":
                    each_selector = (augment_resource_id, ALL_ELEMENTS, i)
                if current_column_name in metadata_dict_left:
                    new_metadata_i = metadata_dict_left[current_column_name]
                else:
                    new_metadata_i = metadata_dict_right[current_column_name]
                metadata_new = metadata_new.update(each_selector, new_metadata_i)

            # start adding shape metadata for dataset
            if return_format == "ds":
                return_df = d3m_DataFrame(df_joined, generate_metadata=False)
                resources = {augment_resource_id: return_df}
                return_result = d3m_Dataset(resources=resources, generate_metadata=False)
                if generate_metadata:
                    return_result.metadata = metadata_new
                    metadata_shape_part_dict = self._generate_metadata_shape_part(value=return_result, selector=())
                    for each_selector, each_metadata in metadata_shape_part_dict.items():
                        return_result.metadata = return_result.metadata.update(selector=each_selector,
                                                                               metadata=each_metadata)
            elif return_format == "df":
                return_result = d3m_DataFrame(df_joined, generate_metadata=False)
                if generate_metadata:
                    return_result.metadata = metadata_new
                    metadata_shape_part_dict = self._generate_metadata_shape_part(value=return_result, selector=())
                    for each_selector, each_metadata in metadata_shape_part_dict.items():
                        return_result.metadata = return_result.metadata.update(selector=each_selector,
                                                                               metadata=each_metadata)

            return return_result

    def get_score(self) -> float:
        return self.score

    def get_metadata(self) -> dict:
        return self.metadata

    def set_join_pairs(self, join_pairs: typing.List[D3MJoinSpec]) -> None:
        """
        manually set up the join pairs
        :param join_pairs: user specified D3MJoinSpec
        :return:
        """
        self.join_pairs = join_pairs

    def get_join_hints(self, supplied_data=None) -> typing.List[D3MJoinSpec]:
        """
        Returns hints for joining supplied data with the data that can be downloaded using this search result.
        In the typical scenario, the hints are based on supplied data that was provided when search was called.

        The optional supplied_data argument enables the caller to request recomputation of join hints for specific data.

        :return: a list of join hints. Note that datamart is encouraged to return join hints but not required to do so.
        """
        if not supplied_data:
            supplied_data = self.supplied_dataframe
        if self.search_type == "general":
            inner_hits = self.search_result.get('inner_hits', {})
            results = []
            used = set()
            for key_path, outer_hits in inner_hits.items():
                vars_type, index, ent_type = key_path.split('.')
                if vars_type != 'required_variables':
                    continue
                left_index = []
                right_index = []
                index = int(index)
                if ent_type == JSONQueryManager.DATAFRAME_COLUMNS:
                    if self.query_json[vars_type][index].get('index'):
                        left_index = self.query_json[vars_type][index].get('index')
                    elif self.query_json[vars_type][index].get('names'):
                        left_index = [supplied_data.columns.tolist().index(idx)
                                      for idx in self.query_json[vars_type][index].get('names')]

                    inner_hits = outer_hits.get('hits', {})
                    hits_list = inner_hits.get('hits')
                    if hits_list:
                        for hit in hits_list:
                            offset = hit['_nested']['offset']
                            if offset not in used:
                                right_index.append(offset)
                                used.add(offset)
                                break

                if left_index and right_index:
                    each_result = D3MJoinSpec(left_columns=left_index, right_columns=right_index)
                    results.append(each_result)

            return results

    @classmethod
    def construct(cls, serialization: dict) -> DatamartSearchResult:
        """
        Take into the serilized input and reconsctruct a "DatamartSearchResult"
        """
        load_result = DatamartSearchResult(search_result=serialization["search_result"],
                                           supplied_data=None,
                                           query_json=serialization["query_json"],
                                           search_type=serialization["search_type"])
        return load_result

    def serialize(self) -> dict:
        output = dict()
        output["search_result"] = self.search_result
        output["query_json"] = self.query_json
        output["search_type"] = self.search_type
        return output


class AugmentSpec:
    """
    Abstract class for D3M augmentation specifications
    """


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
    def __init__(self, left_resource_id: str, right_resource_id: str, left_columns: typing.List[typing.List[DatasetColumn]],
                 right_columns: typing.List[typing.List[DatasetColumn]]) -> None:
        self.left_resource_id = left_resource_id
        self.right_resource_id = right_resource_id
        self.left_columns = left_columns
        self.right_columns = right_columns
        # we can have list of the joining column pairs
        # each list inside left_columns/right_columns is a candidate joining column for that dataFrame
        # each candidate joining column can also have multiple columns


class UnionSpec(AugmentSpec):
    """
    A union spec specifies how to combine rows of a dataframe in the left dataset with a dataframe in the right dataset.
    The dataframe after union should have the same columns as the left dataframe.

    Implementation: TBD
    """
    pass


class TemporalGranularity(utils.Enum):
    YEAR = 1
    MONTH = 2
    DAY = 3
    HOUR = 4
    SECOND = 5


class GeospatialGranularity(utils.Enum):
    COUNTRY = 1
    STATE = 2
    COUNTY = 3
    CITY = 4
    POSTAL_CODE = 5


class ColumnRelationship(utils.Enum):
    CONTAINS = 1
    SIMILAR = 2
    CORRELATED = 3
    ANTI_CORRELATED = 4
    MUTUALLY_INFORMATIVE = 5
    MUTUALLY_UNINFORMATIVE = 6


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
    def __init__(self, keywords: typing.List[str]=[], variables: typing.List['VariableConstraint']=[], search_type: str="datamart") -> None:
        self.search_type = search_type
        self.keywords = keywords
        self.variables = variables


class VariableConstraint(object):
    """
    Abstract class for all variable constraints.
    """


class NamedEntityVariable(VariableConstraint):
    """
    Specifies that a matching dataset must contain a variable including the specified set of named entities.

    For example, if the entities are city names, the expectation is that a matching dataset must contain a variable
    (column) with the given city names. Due to spelling differences and incompleteness of datasets, the returned
    results may not contain all the specified entities.

    Parameters
    ----------
    entities : List[str]
        List of strings that should be contained in the matched dataset column.
    """
    def __init__(self, entities: typing.List[str]) -> None:
        self.entities = entities


class TemporalVariable(VariableConstraint):
    """
    Specifies that a matching dataset should contain a variable with temporal information (e.g., dates) satisfying
    the given constraint.

    The goal is to return a dataset that covers the requested temporal interval and includes
    data at a requested level of granularity.

    Datamart will return best effort results, including datasets that may not fully cover the specified temporal
    interval or whose granularity is finer or coarser than the requested granularity.

    Parameters
    ----------
    start : datetime
        A matching dataset should contain a variable with temporal information that starts earlier than the given start.
    end : datetime
        A matching dataset should contain a variable with temporal information that ends after the given end.
    granularity : TemporalGranularity
        A matching dataset should provide temporal information at the requested level of granularity.
    """
    def __init__(self, start: datetime.datetime, end: datetime.datetime, granularity: TemporalGranularity = None) -> None:
        self.start = start
        self.end = end
        self.granularity = granularity


class GeospatialVariable(VariableConstraint):
    """
    Specifies that a matching dataset should contain a variable with geospatial information that covers the given
    bounding box.

    A matching dataset may contain variables with latitude and longitude information (in one or two columns) that
    cover the given bounding box.

    Alternatively, a matching dataset may contain a variable with named entities of the given granularity that provide
    some coverage of the given bounding box. For example, if the bounding box covers a 100 mile square in Southern
    California, and the granularity is City, the result should contain Los Angeles, and other cities in Southern
    California that intersect with the bounding box (e.g., Hawthorne, Torrance, Oxnard).

    Parameters
    ----------
    latitude1 : float
        The latitude of the first point
    longitude1 : float
        The longitude of the first point
    latitude2 : float
        The latitude of the second point
    longitude2 : float
        The longitude of the second point
    granularity : GeospatialGranularity
        Requested geospatial values are well matched with the requested granularity
    """
    def __init__(self, latitude1: float, longitude1: float, latitude2: float, longitude2: float, granularity: GeospatialGranularity = None) -> None:
        self.latitude1 = latitude1
        self.longitude1 = longitude1
        self.latitude2 = latitude2
        self.longitude2 = longitude2
        self.granularity = granularity


class TabularVariable(object):
    """
    Specifies that a matching dataset should contain variables related to given columns in the supplied_dataset.

    The relation ColumnRelationship.CONTAINS specifies that string values in the columns overlap using the string
    equality comparator. If supplied_dataset columns consists of temporal or spatial values, then
    ColumnRelationship.CONTAINS specifies overlap in temporal range or geospatial bounding box, respectively.

    The relation ColumnRelationship.SIMILAR specifies that string values in the columns overlap using fuzzy string matching.

    The relations ColumnRelationship.CORRELATED and ColumnRelationship.ANTI_CORRELATED specify the columns are
    correlated and anti-correlated, respectively.

    The relations ColumnRelationship.MUTUALLY_INFORMATIVE and ColumnRelationship.MUTUALLY_UNINFORMATIVE specify the columns
    are mutually and anti-correlated, respectively.

    Parameters:
    -----------
    columns : typing.List[int]
        Specify columns in the dataframes of the supplied_dataset
    relationship : ColumnRelationship
        Specifies how the the columns in the supplied_dataset are related to the variables in the matching dataset.
    """
    def __init__(self, columns: typing.List[DatasetColumn], relationship: ColumnRelationship) -> None:
        self.columns = columns
        self.relationship = relationship
