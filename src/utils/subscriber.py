import json
from typing import List, Dict, Any
from time import perf_counter
import re
import traceback
import logging

from azure.servicebus import ServiceBusClient


# os.environ["HTTP_PROXY"] = "http://proxy-dmz.intel.com:912"
# os.environ["HTTPS_PROXY"] = "http://proxy-dmz.intel.com:912"

data_logger = logging.getLogger(__name__)

def exec_single_matched_file(matched_file, matched_file_size, queries, axon_id):
    file_matches: List[Dict[str, Any]] = []
    query_results: List[Dict[str, Any]] = []
    perf_start = perf_counter()

    local_path = f"../../Local/Shared_files/{axon_id}/"

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


class Subscriber:

    @property
    def servicebus_client(self):
        return self._servicebus_client

    @servicebus_client.setter
    def servicebus_client(self, value):
        self._servicebus_client = value

    # -------------------------------------------
    @property
    def receiver(self):
        return self._receiver

    @receiver.setter
    def receiver(self, value):
        self._receiver = value

    # ---------------------------------------------------------------------
    def __init__(self, l2_utils):
        # Initialize values
        connection_string = l2_utils.get_kv_secret("EGProcessQueueEndPoint").value
        queue_name = l2_utils.get_kv_secret("EGProcessQueueName").value
        self.init_receiver(connection_string, queue_name)

    def init_receiver(self, connection_string, queue_name):

        HTTP_PROXY = {
            'proxy_hostname': 'proxy-dmz.intel.com',  # proxy hostname.
            'proxy_port': 912  # proxy port.
        }
        self.servicebus_client = ServiceBusClient.from_connection_string(conn_str=connection_string,
                                                                         logging_enable=True, http_proxy=HTTP_PROXY)

        self.receiver = self.servicebus_client.get_queue_receiver(queue_name=queue_name)
        # return

    def poll_messages(self):
        received_msgs = self.receiver.receive_messages(max_message_count=1, max_wait_time=5)
        for msg in received_msgs:

            stat = self.process_message(msg)
            if stat == 0:
                self.complete_message(msg)
            else:
                pass
                # todo: need to decide what to do with failed messages
                # it means that a file could not be parsed for some reason

    def process_message(self, queue_msg):
        try:
            msg = json.loads(str(queue_msg))
            query_results = exec_single_matched_file(msg["file_name"], msg["file_size"], msg["queries"],
                                                     msg["axon_id"])

        except Exception as exc:
            data_logger.error(f"{msg['file_name']} generated an exception: {exc}")
            data_logger.error(f"Traceback: {traceback.format_exc()}")

        else:
            data_logger.info(f"{msg['file_name']} search completed successfully")
            # todo: add writer to destination
            # todo: add db status writer
            return 0

    def complete_message(self, msg):
        self.receiver.complete_message(msg)

    def empty_queue(self):

        received_msgs = self.receiver.receive_messages(max_message_count=1000, max_wait_time=500000)
        for msg in received_msgs:
            self.receiver.complete_message(msg)
