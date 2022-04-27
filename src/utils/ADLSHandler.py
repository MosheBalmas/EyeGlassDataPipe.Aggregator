import os
import traceback
import logging

from azure.storage.filedatalake import DataLakeServiceClient

os.environ["HTTP_PROXY"] = "http://proxy-dmz.intel.com:912"
os.environ["HTTPS_PROXY"] = "http://proxy-dmz.intel.com:912"

data_logger = logging.getLogger(__name__)

class ADLSHandler:

    @property
    def ADLS_account_end_point(self):
        return self._ADLS_account_end_point

    @ADLS_account_end_point.setter
    def ADLS_account_end_point(self, value):
        self._ADLS_account_end_point = value

    @property
    def ADLS_account_name(self):
        return self._ADLS_account_name

    @ADLS_account_name.setter
    def ADLS_account_name(self, value):
        self._ADLS_account_name = value

    @property
    def ADLS_account_key(self):
        return self._ADLS_account_key

    @ADLS_account_key.setter
    def ADLS_account_key(self, value):
        self._ADLS_account_key = value

    @property
    def ADLS_account_container(self):
        return self._ADLS_account_container

    @ADLS_account_container.setter
    def ADLS_account_container(self, value):
        self._ADLS_account_container = value

    @property
    def connection_string(self):
        return self._connection_string

    @connection_string.setter
    def connection_string(self, value):
        self._connection_string = value

    @property
    def service_client(self):
        return self._service_client

    @service_client.setter
    def service_client(self, value):
        self._service_client = value

    @property
    def file_system_client(self):
        return self._file_system_client

    @file_system_client.setter
    def file_system_client(self, value):
        self._file_system_client = value

    @property
    def directory_client(self):
        return self._directory_client

    @directory_client.setter
    def directory_client(self, value):
        self._directory_client = value

    @property
    def fs_paths(self):
        return self._fs_paths

    @fs_paths.setter
    def fs_paths(self, value):
        self._fs_paths = value

    # --------------------------------------------------------------------------------
    def __init__(self, l2_utils, mode="Prod", adls_props=None):

        if adls_props is None:
            adls_props = {"end_point": "ADLSAccountEndPoint",
                          "container": "ADLSAccountContainerTest",
                          "test_container": "ADLSAccountContainerTest"}
        try:

            self.l2_utils = l2_utils
            data_logger.info("Utils handler created")
            self.ADLS_account_end_point = l2_utils.get_kv_secret(adls_props["end_point"]).value
            self.ADLS_account_container = l2_utils.get_kv_secret(
                adls_props["test_container"]).value if mode == "test" else l2_utils.get_kv_secret(
                adls_props["container"]).value

            data_logger.info("ADLS Connection data fetched from KV")

            self.initialize_storage_account()
            self.initialize_container()

        except Exception as e:
            data_logger.error(f"ADLS handler init failed. Error: {str(e)}. Traceback:. {traceback.format_exc()}")
            raise

    # --------------------------------------------------------------------------------

    def initialize_storage_account(self):

        try:
            self.service_client = DataLakeServiceClient.from_connection_string(self.ADLS_account_end_point)
            data_logger.info(f"DataLakeServiceClient created")
        except Exception as e:
            data_logger.error(f"Initialize_storage_account: Exception of type %s occurred. Error: {str(e)}. Traceback:. {traceback.format_exc()}")

    # --------------------------------------------------------------------------------

    def initialize_container(self):
        try:

            self.file_system_client = self.service_client.get_file_system_client(
                file_system=self.ADLS_account_container)
            data_logger.info("ADLS file system initialized: %s" % self.file_system_client)

            paths = self.file_system_client.get_paths()

            self.fs_paths = [p for p in paths]

            # print (self.fs_paths)
        except Exception as e:
            data_logger.error(
                f"Initialize_container: Exception of type %s occurred. Error: {str(e)}. Traceback:. {traceback.format_exc()}")

    # --------------------------------------------------------------------------------

    def create_directory(self, directory):
        try:
            self.directory_client = self.file_system_client.create_directory(directory)
            data_logger.info("Directory created: %s" % (directory))

        except Exception as e:
            data_logger.error(
                f"Create directory: Exception of type %s occurred. Error: {str(e)}. Traceback:. {traceback.format_exc()}")

    # --------------------------------------------------------------------------------

    def create_file(self, file_name, file_blob):
        try:

            file_client = self.directory_client.create_file(file_name)
            file_client.append_data(data=file_blob, offset=0, length=len(file_blob))

            file_client.flush_data(len(file_blob))

            data_logger.info(f"File created: {file_name}")

        except Exception as e:
            data_logger.error(
                f"Create file: Exception of type %s occurred. Error: {str(e)}. Traceback:. {traceback.format_exc()}")
