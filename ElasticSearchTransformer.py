#!/usr/bin/env python3

import json
import tempfile

from elasticsearch import Elasticsearch
from elasticsearch import helpers


class ElasticSearchTransformer:
    MATCH_ALL_QUERY = {
        "query": {"match_all": {}}
    }

    def __init__(self, elastic_search_url):
        self.elastic_search_client = Elasticsearch(elastic_search_url)

    def transform(self, original_index, transformed_index):
        with tempfile.NamedTemporaryFile() as temp_file:
            self.__transform_and_write_output_into_file(original_index, temp_file.name)
            self.__load_transformed_data_into_transformed_index(transformed_index, temp_file.name)

    def __transform_and_write_output_into_file(self, index_name, file_name):
        with open(file_name, "a") as file:
            scanner = helpers.scan(
                self.elastic_search_client,
                index=index_name,
                query=self.MATCH_ALL_QUERY
            )
            for document in scanner:
                original_record = document['_source']
                transformed_record = dict(
                    original_record,
                    calculated=self.__calculate_number_of_characters_in_keys_and_values(original_record)
                )
                file.write(json.dumps(transformed_record) + '\n')

    def __load_transformed_data_into_transformed_index(self, transformed_index, file_name):
        self.elastic_search_client.indices.create(index=transformed_index)
        helpers.bulk(
            self.elastic_search_client,
            self.__generate_docs_from_file(file_name, transformed_index)
        )

    @staticmethod
    def __generate_docs_from_file(file_name, index_name):
        with open(file_name, "r") as file:
            for row in file:
                doc = {
                    "_index": index_name,
                    "_source": json.loads(row),
                }
                yield doc

    @staticmethod
    def __calculate_number_of_characters_in_keys_and_values(record):
        number_of_characters = 0
        for key, value in record.items():
            number_of_characters += len(str(key))
            number_of_characters += len(str(value))
        return number_of_characters
