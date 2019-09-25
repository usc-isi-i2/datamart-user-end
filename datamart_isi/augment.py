from datamart_isi.profiler import Profiler
import pandas as pd
import typing
import warnings
import traceback
import logging
from datetime import datetime
from datamart_isi.utilities.utils import Utils
from datamart_isi.joiners.joiner_base import JoinerPrepare, JoinerType
from datamart_isi.joiners.join_result import JoinResult
from datamart_isi.utilities import connection
from SPARQLWrapper import SPARQLWrapper, JSON, POST, URLENCODED
from itertools import chain
from datamart_isi.utilities.geospatial_related import GeospatialRelated
from datamart_isi.cache.wikidata_cache import QueryCache


class Augment(object):

    def __init__(self, endpoint: str) -> None:
        """Init method of QuerySystem, set up connection to elastic search.

        Args:
            endpoint: query endpoint address

        Returns:

        """

        self.qm = SPARQLWrapper(connection.get_general_search_server_url(endpoint))
        self.qm.setReturnFormat(JSON)
        self.qm.setMethod(POST)
        self.qm.setRequestMethod(URLENCODED)
        self.profiler = Profiler()
        self.logger = logging.getLogger(__name__)
        self.wikidata_cache_manager = QueryCache(connection_url=endpoint)

    def query_by_sparql(self, query: dict, dataset: pd.DataFrame = None) -> typing.Optional[typing.List[dict]]:
        """
        Args:
            query: a dictnary format query
            dataset: 
            **kwargs:

        Returns:

        """
        if query:
            query_body = self.parse_sparql_query(query, dataset)
            try:
                self.qm.setQuery(query_body)
                results = self.qm.query().convert()['results']['bindings']
            except Exception as e:
                self.logger.error(e, exc_info=True)
                traceback.print_exc()
                return []
            return results
        else:
            print("\n\n[ERROR] No query given, query failed!\n\n")
            return []

    def parse_sparql_query(self, json_query, dataset) -> str:
        """
        parse the json query to a spaqrl query format
        :param json_query:
        :param dataset:
        :return: a string indicate the sparql query
        """
        # example of query variables: Chaves Los Angeles Sacramento
        PREFIX = '''
            prefix ps: <http://www.wikidata.org/prop/statement/>
            prefix pq: <http://www.wikidata.org/prop/qualifier/> 
            prefix p: <http://www.wikidata.org/prop/>
        '''
        SELECTION = '''
            SELECT ?dataset ?datasetLabel ?variableName ?variable ?score ?rank ?url ?file_type ?title ?start_time ?end_time ?time_granularity ?keywords ?extra_information
        '''
        STRUCTURE = '''
            WHERE {
                ?dataset rdfs:label ?datasetLabel.
                ?dataset p:P2699/ps:P2699 ?url.
                ?dataset p:P2701/ps:P2701 ?file_type.
                ?dataset p:C2010/ps:C2010 ?extra_information.
                ?dataset p:C2005 ?variable.
                ?variable ps:C2005 ?variableName.
                ?dataset p:P1476 ?title_url.
                ?title_url ps:P1476 ?title .
                ?dataset p:C2004 ?keywords_url.
                ?keywords_url ps:C2004 ?keywords.
        '''
        bind = ""
        ORDER = "ORDER BY DESC(?score)"
        LIMIT = "LIMIT 10"
        spaqrl_query = PREFIX + SELECTION + STRUCTURE

        if "variables" in json_query.keys() and json_query["variables"] != {}:
            query_variables = json_query['variables']
            query_part = " ".join(query_variables.values())
            spaqrl_query += '''
                ?variable pq:C2006 [
                            bds:search """''' + query_part + '''""" ;
                            bds:relevance ?score_var ;
                          ].
                '''
            bind = "?score_var" if bind == "" else bind + "+ ?score_var"

        if "keywords_search" in json_query.keys() and json_query["keywords_search"] != []:
            query_keywords = json_query["keywords_search"]
            query_part = " ".join(query_keywords)
            spaqrl_query += '''
                ?keywords_url ps:C2004 [
                                bds:search """''' + query_part + '''""" ;
                                bds:relevance ?score_key ;
                              ].
                '''
            bind = "?score_key" if bind == "" else bind + "+ ?score_key"

        if "variables_search" in json_query.keys() and json_query["variables_search"] != {}:
            if "temporal_variable" in json_query["variables_search"].keys():
                tv = json_query["variables_search"]["temporal_variable"]
                temporal_granularity = {'second': 14, 'minute': 13, 'hour': 12, 'day': 11, 'month': 10, 'year': 9}

                start_date = pd.to_datetime(tv["start"]).isoformat()
                end_date = pd.to_datetime(tv["end"]).isoformat()
                granularity = temporal_granularity[tv["granularity"]]
                spaqrl_query += '''
                    ?variable pq:C2013 ?time_granularity . 
                    ?variable pq:C2011 ?start_time .
                    ?variable pq:C2012 ?end_time . 
                    FILTER(?time_granularity >= ''' + str(granularity) + ''') 
                    FILTER(!((?start_time > "''' + end_date + '''"^^xsd:dateTime) || (?end_time < "''' + start_date + '''"^^xsd:dateTime)))
                    '''

            if "geospatial_variable" in json_query["variables_search"].keys():
                geo_variable = json_query["variables_search"]["geospatial_variable"]
                qnodes = self.parse_geospatial_query(geo_variable)
                if qnodes:
                    # find similar dataset from datamart
                    query_part = " ".join(qnodes)
                    # query_part = "q1494 q1400 q759 q1649 q1522 q1387 q16551" # COMMENT: for testing
                    spaqrl_query += '''
                                        ?variable pq:C2006 [
                                            bds:search """''' + query_part + '''""" ;
                                            bds:relevance ?score_geo ;
                                        ].
                                     '''
                    bind = "?score_geo" if bind == "" else bind + "+ ?score_geo"

        # if "title_search" in json_query.keys() and json_query["title_search"] != '':
        #     query_title = json_query["title_search"]
        #     spaqrl_query += '''
        #         ?title_url ps:P1476 [
        #                   bds:search """''' + query_title + '''""" ;
        #                   bds:relevance ?score_title ;
        #                 ].
        #     '''
        #     bind = "?score_title" if bind == "" else bind + "+ ?score_title"
        if bind:
            spaqrl_query += "\n BIND((" + bind + ") AS ?score)"

        spaqrl_query += "\n }" + "\n" + ORDER + "\n" + LIMIT

        return spaqrl_query

    def parse_geospatial_query(self, geo_variable):
        geo_gra_dict = {'country': 'Q6256', 'state': 'Q7275', 'city': 'Q515', 'county': 'Q28575',
                        'postal_code': 'Q37447'}
        qnodes = set()

        # located inside a bounding box
        if "latitude1" in geo_variable.keys() and "latitude2" in geo_variable.keys():
            geo1_related = GeospatialRelated(float(geo_variable["latitude1"]), float(geo_variable["longitude1"]))
            geo1_related.coordinate_transform()  # axis transformation
            geo2_related = GeospatialRelated(float(geo_variable["latitude2"]), float(geo_variable["longitude2"]))
            geo2_related.coordinate_transform()
            # find top left point and bottom right point
            top_left_point, botm_right_point = geo1_related.distinguish_two_points(geo2_related)
            granularity = geo_gra_dict[geo_variable["granularity"]]

            if top_left_point and botm_right_point:
                # get Q nodes located inside a geospatial bounding box from wikidata query
                sparql_query = "select distinct ?place where \n{\n  ?place wdt:P31/wdt:P279* wd:" + granularity + " .\n" \
                               + "SERVICE wikibase:box {\n ?place wdt:P625 ?location .\n" \
                               + "bd:serviceParam wikibase:cornerWest " + "\"Point(" + str(
                    top_left_point[0]) + " " + str(top_left_point[1]) + ")\"^^geo:wktLiteral .\n" \
                               + "bd:serviceParam wikibase:cornerEast " + "\"Point(" + str(
                    botm_right_point[0]) + " " + str(botm_right_point[1]) + ")\"^^geo:wktLiteral .\n}\n" \
                               + "SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\" }\n}\n"
                results = self.wikidata_cache_manager.get_result(sparql_query)
                if results:
                    for each in results:
                        value = each["place"]["value"]
                        value = value.split('/')[-1]
                        qnodes.add(value)

        return qnodes

    #
    # def query(self,
    #           col: pd.Series = None,
    #           minimum_should_match_ratio_for_col: float = None,
    #           query_string: str = None,
    #           temporal_coverage_start: str = None,
    #           temporal_coverage_end: str = None,
    #           global_datamart_id: int = None,
    #           variable_datamart_id: int = None,
    #           key_value_pairs: typing.List[tuple] = None,
    #           **kwargs
    #           ) -> typing.Optional[typing.List[dict]]:
    #
    #     """Query metadata by a pandas Dataframe column
    #
    #     Args:
    #         col: pandas Dataframe column.
    #         minimum_should_match_ratio_for_col: An float ranges from 0 to 1
    #             indicating the ratio of unique value of the column to be matched
    #         query_string: string to query any field in metadata
    #         temporal_coverage_start: start of a temporal coverage
    #         temporal_coverage_end: end of a temporal coverage
    #         global_datamart_id: match a global metadata id
    #         variable_datamart_id: match a variable metadata id
    #         key_value_pairs: match key value pairs
    #
    #     Returns:
    #         matching docs of metadata
    #     """
    #
    #     queries = list()
    #
    #     if query_string:
    #         queries.append(
    #             self.qm.match_any(query_string=query_string)
    #         )
    #
    #     if temporal_coverage_start or temporal_coverage_end:
    #         queries.append(
    #             self.qm.match_temporal_coverage(start=temporal_coverage_start, end=temporal_coverage_end)
    #         )
    #
    #     if global_datamart_id:
    #         queries.append(
    #             self.qm.match_global_datamart_id(datamart_id=global_datamart_id)
    #         )
    #
    #     if variable_datamart_id:
    #         queries.append(
    #             self.qm.match_variable_datamart_id(datamart_id=variable_datamart_id)
    #         )
    #
    #     if key_value_pairs:
    #         queries.append(
    #             self.qm.match_key_value_pairs(key_value_pairs=key_value_pairs)
    #         )
    #
    #     if col is not None:
    #         queries.append(
    #             self.qm.match_some_terms_from_variables_array(terms=col.unique().tolist(),
    #                                                           minimum_should_match=minimum_should_match_ratio_for_col)
    #         )
    #
    #     if not queries:
    #         return self._query_all()
    #
    #     return self.qm.search(body=self.qm.form_conjunction_query(queries), **kwargs)
    #
    # def join(self,
    #          left_df: pd.DataFrame,
    #          right_df: pd.DataFrame,
    #          left_columns: typing.List[typing.List[int]],
    #          right_columns: typing.List[typing.List[int]],
    #          left_metadata: dict = None,
    #          right_metadata: dict = None,
    #          joiner: JoinerType = JoinerType.DEFAULT
    #          ) -> JoinResult:
    #
    #     """Join two dataframes based on different joiner.
    #
    #       Args:
    #           left_df: pandas Dataframe
    #           right_df: pandas Dataframe
    #           left_metadata: metadata of left dataframe
    #           right_metadata: metadata of right dataframe
    #           left_columns: list of integers from left df for join
    #           right_columns: list of integers from right df for join
    #           joiner: string of joiner, default to be "default"
    #
    #       Returns:
    #            JoinResult
    #       """
    #
    #     if joiner not in self.joiners:
    #         self.joiners[joiner] = JoinerPrepare.prepare_joiner(joiner=joiner)
    #
    #     if not self.joiners[joiner]:
    #         warnings.warn("No suitable joiner, return original dataframe")
    #         return JoinResult(left_df, [])
    #
    #     print(" - start profiling")
    #     if not (left_metadata and left_metadata.get("variables")):
    #         # Left df is the user provided one.
    #         # We will generate metadata just based on the data itself, profiling and so on
    #         left_metadata = Utils.generate_metadata_from_dataframe(data=left_df, original_meta=left_metadata)
    #
    #     if not right_metadata:
    #         right_metadata = Utils.generate_metadata_from_dataframe(data=right_df)
    #
    #     # Only profile the joining columns, otherwise it will be too slow:
    #     left_metadata = Utils.calculate_dsbox_features(data=left_df, metadata=left_metadata,
    #                                                    selected_columns=set(chain.from_iterable(left_columns)))
    #
    #     right_metadata = Utils.calculate_dsbox_features(data=right_df, metadata=right_metadata,
    #                                                     selected_columns=set(chain.from_iterable(right_columns)))
    #
    #     # update with implicit_variable on the user supplied dataset
    #     if left_metadata.get('implicit_variables'):
    #         Utils.append_columns_for_implicit_variables_and_add_meta(left_metadata, left_df)
    #
    #     print(" - start joining tables")
    #     res = self.joiners[joiner].join(left_df=left_df,
    #                                     right_df=right_df,
    #                                     left_columns=left_columns,
    #                                     right_columns=right_columns,
    #                                     left_metadata=left_metadata,
    #                                     right_metadata=right_metadata,
    #                                     )
    #
    #     return res
