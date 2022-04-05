#!/usr/bin/env python
# coding: utf-8
import os
from typing import List, Dict, Any
from time import perf_counter
import re

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential, ChainedTokenCredential, ClientSecretCredential

from src.utils.L2Logger import L2Logger

os.environ["HTTP_PROXY"] = "proxy-dmz.intel.com:912"
os.environ["HTTPS_PROXY"] = "proxy-dmz.intel.com:912"

os.environ["AZURE_SUBSCRIPTION_ID"] = "d3e4d481-e08a-4d85-b01a-9ae56dab1e72"
os.environ["AZURE_TENANT_ID"] = "46c98d88-e344-4ed4-8496-4ed7712e255d"
os.environ["AZURE_CLIENT_ID"] = "e4d776c8-8bbf-4761-b9e9-5691aa4a3d0a"
os.environ["AZURE_CLIENT_SECRET"] = "ygt7Q~5iZXsj2XeR6M6jKBOkyesjFBp_SpJEb"

def exec_single_matched_file(matched_file, matched_file_size, queries, axon_id):
    file_matches: List[Dict[str, Any]] = []
    query_results: List[Dict[str, Any]] = []
    perf_start = perf_counter()

    local_path = f"./Local/Shared_files/{axon_id}/"

    with open(local_path + matched_file, 'r') as file:
        file_data = file.read()

        for data_query in queries:
            # Each file matching any files regex pattern may be related to more than a single data query
            # Need to loop over all related queries and find the data pattern matches per each
            flags = 0x0 if data_query["Case_Sensitive"] == 1 else re.IGNORECASE
            query_type = "error" if data_query["Query_Type"] is None or data_query["Query_Type"] == "nan" else \
                data_query["Query_Type"]

            data_re = re.compile(data_query["Data_RegEx"], flags)

            match = data_re.finditer(file_data)

            for m in match:
                m_groups = m.groups(0)
                for m_group in m_groups:
                    # Collect all the signatures found in the current file for the current data query
                    file_matches.append({"match_text": m_group})

            query_results.append({"query_id": data_query["Query_Id"],
                                  "query_name": data_query["Query_Name"],
                                  "query_type": query_type,
                                  "query_version": data_query["Query_Version"],

                                  "folder_regex": data_query["Folder_RegEx"],
                                  "file_regex": data_query["File_RegEx"],
                                  "data_regex": data_query["Data_RegEx"],

                                  "file_name": matched_file,
                                  "file_size": matched_file_size,
                                  "duration": perf_counter() - perf_start,
                                  "signatures": file_matches,
                                  }
                                 )

    return query_results


class L2_Utils:

    @property
    def kv_client(self):
        return self._kv_client

    @kv_client.setter
    def kv_client(self, value):
        self._kv_client = value

    def __init__(self):
        self.setup_kv_client()

    def setup_kv_client(self):
        keyVaultName = "aiacdataengkvdev"
        KVUri = f"https://{keyVaultName}.vault.azure.net"
        HTTP_PROXY = {
            'proxy_hostname': 'proxy-dmz.intel.com'  # proxy hostname.
            , 'proxy_port': 912  # proxy port.

        }



        credentials = None
        try:
            credentials = DefaultAzureCredential()
        except Exception as e:
            pass

        self.kv_client = SecretClient(vault_url=KVUri, credential=credentials)  # , proxies =HTTP_PROXY )

    def get_kv_secret(self, secret_name):

        return self.kv_client.get_secret(secret_name)


    @staticmethod
    def GetProxySettings(self):

        http_proxy = self.get_kv_secret("AxonHttpProxy").value
        https_proxy = self.get_kv_secret("AxonHttpsProxy").value

        # print (http_proxy,https_proxy)
        return {"http": http_proxy,
                "https": https_proxy}
