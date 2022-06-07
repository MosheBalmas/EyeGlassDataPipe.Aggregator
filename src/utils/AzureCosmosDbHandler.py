import logging
import traceback
import json

import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions

global data_logger


class AzureCosmosDbHandler:

    @property
    def CosmosDbHost(self):
        return self._CosmosDbHost

    @CosmosDbHost.setter
    def CosmosDbHost(self, value):
        self._CosmosDbHost = value

    @property
    def CosmosDbMasterKey(self):
        return self._CosmosDbMasterKey

    @CosmosDbMasterKey.setter
    def CosmosDbMasterKey(self, value):
        self._CosmosDbMasterKey = value

    @property
    def CosmosDbDatabaseId(self):
        return self._CosmosDbDatabaseId

    @CosmosDbDatabaseId.setter
    def CosmosDbDatabaseId(self, value):
        self._CosmosDbDatabaseId = value

    @property
    def CosmosDbContainerId(self):
        return self._CosmosDbContainerId

    @CosmosDbContainerId.setter
    def CosmosDbContainerId(self, value):
        self._CosmosDbContainerId = value

    @property
    def client(self):
        return self._client

    @client.setter
    def client(self, value):
        self._client = value

    @property
    def db(self):
        return self._db

    @db.setter
    def db(self, value):
        self._db = value

    @property
    def container(self):
        return self._container

    @container.setter
    def container(self, value):
        self._container = value
        # --------------------------------------------------------------------------------

    def __init__(self, l2_utils, data_logger_p=logging.getLogger(__name__)):

        try:
            global data_logger
            data_logger = data_logger_p

            data_logger.info(f"cosmos DB initiated")

            self.CosmosDbHost = l2_utils.get_kv_secret(
                "CosmosDbHost").value
            self.CosmosDbMasterKey = l2_utils.get_kv_secret(
                "CosmosDbMasterKey").value

            self.CosmosDbDatabaseId = l2_utils.get_kv_secret("CosmosDbDatabaseId").value
            self.CosmosDbContainerId = l2_utils.get_kv_secret("CosmosDbContainerId").value

            data_logger.info(
                f"Connecting Azure Cosmos document DB {self.CosmosDbDatabaseId}/{self.CosmosDbContainerId}")
            # self.SetHandlerConnection()


        except Exception as e:
            data_logger.error(f"Cosmos DB handler init failed. Error: {traceback.format_exc()}")
            raise

    def SetHandlerConnection(self):
        self.client = cosmos_client.CosmosClient(self.CosmosDbHost, {'masterKey': self.CosmosDbMasterKey})
        try:
            # setup database
            self.db = self.client.get_database_client(self.CosmosDbDatabaseId)
            # setup container
            self.container = self.db.get_container_client(self.CosmosDbContainerId)

        except exceptions.CosmosHttpResponseError as e:
            # print('AzureCosmosDbHandler : SetHandlerConnection failed. {0}'.format(e.message))
            data_logger.error('AzureCosmosDbHandler : SetHandlerConnection failed. {0}'.format(e.message))

    def create_items(self, doc):

        try:

            ret = self.container.create_item(body=doc)
            data_logger.info("Cosmos DB item created: {}".format(ret))
        except exceptions.CosmosHttpResponseError as e:
            # the item already exists
            # print("AzureCosmosDbHandler : Item already exists. content replaced" )
            data_logger.warning("AzureCosmosDbHandler : Item already exists. content replaced")
            response = self.container.read_item(item=doc["id"], partition_key=doc["id"])
            self.container.replace_item(item=response, body=doc)

    def count_items_created(self, uuid_list):
        items = []
        with cosmos_client.CosmosClient(self.CosmosDbHost, {'masterKey': self.CosmosDbMasterKey}) as cosmos_cl:
            try:

                # setup database
                db = cosmos_cl.get_database_client(self.CosmosDbDatabaseId)

                # setup container
                container = db.get_container_client(self.CosmosDbContainerId)

                items = list(container.query_items(
                    query=f"SELECT c.p_uuid  FROM c WHERE c.p_uuid in ({json.dumps(uuid_list)[1:-1]})",  #json.dumps(result_items)[1:-1]})",
                    enable_cross_partition_query=True

                ))
            except exceptions.CosmosHttpResponseError as e:
                data_logger.error('AzureCosmosDbHandler : SetHandlerConnection failed. {0}'.format(e.message))
        # print(items)
        return len(items)

    def get_all_items(self, uuid_list):
        items = []
        with cosmos_client.CosmosClient(self.CosmosDbHost, {'masterKey': self.CosmosDbMasterKey}) as cosmos_cl:
            try:

                # setup database
                db = cosmos_cl.get_database_client(self.CosmosDbDatabaseId)

                # setup container
                container = db.get_container_client(self.CosmosDbContainerId)

                items = list(container.query_items(
                    query=f"SELECT c.p_uuid, c.duration, c.file_name, c.matched_file_size, c.query_status, "
                          f"c.query_msg, c.query_results  FROM c WHERE c.p_uuid in ({json.dumps(uuid_list)[1:-1]})",
                    enable_cross_partition_query=True

                ))
            except exceptions.CosmosHttpResponseError as e:
                data_logger.error('AzureCosmosDbHandler : SetHandlerConnection failed. {0}'.format(e.message))
        # print(items)
        return items
