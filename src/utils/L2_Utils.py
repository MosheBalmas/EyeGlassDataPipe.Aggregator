#!/usr/bin/env python
# coding: utf-8
import os

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential, ChainedTokenCredential, ClientSecretCredential

from src.utils.L2Logger import L2Logger

os.environ["HTTP_PROXY"] = "proxy-dmz.intel.com:912"
os.environ["HTTPS_PROXY"] = "proxy-dmz.intel.com:912"

os.environ["AZURE_SUBSCRIPTION_ID"] = "d3e4d481-e08a-4d85-b01a-9ae56dab1e72"
os.environ["AZURE_TENANT_ID"] = "46c98d88-e344-4ed4-8496-4ed7712e255d"
os.environ["AZURE_CLIENT_ID"] = "e4d776c8-8bbf-4761-b9e9-5691aa4a3d0a"
os.environ["AZURE_CLIENT_SECRET"] = "ygt7Q~5iZXsj2XeR6M6jKBOkyesjFBp_SpJEb"


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
