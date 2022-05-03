import json
import time
import traceback
import logging
from time import perf_counter
import datetime

import azure.servicebus
from azure.servicebus import ServiceBusClient, ServiceBusMessage, AutoLockRenewer

from src.utils.AzureSqlHandler import AzureSqlHandler
from src.utils.ADLSHandler import ADLSHandler

# os.environ["HTTP_PROXY"] = "http://proxy-dmz.intel.com:912"
# os.environ["HTTPS_PROXY"] = "http://proxy-dmz.intel.com:912"

global data_logger


class PubSubHandler:

    @property
    def servicebus_client(self):
        return self._servicebus_client

    @servicebus_client.setter
    def servicebus_client(self, value):
        self._servicebus_client = value

    # -------------------------------------------
    @property
    def l2_utils(self):
        return self._l2_utils

    @l2_utils.setter
    def l2_utils(self, value):
        self._l2_utils = value

    # -------------------------------------------
    @property
    def sql_handler(self):
        return self._sql_handler

    @sql_handler.setter
    def sql_handler(self, value):
        self._sql_handler = value

    # -------------------------------------------
    @property
    def aggregators_receiver(self):
        return self._aggregators_receiver

    @aggregators_receiver.setter
    def aggregators_receiver(self, value):
        self._aggregators_receiver = value

    @property
    def connection_string(self):
        return self._connection_string

    @connection_string.setter
    def connection_string(self, value):
        self._connection_string = value

    @property
    def receiver_queue_name(self):
        return self._receiver_queue_name

    @receiver_queue_name.setter
    def receiver_queue_name(self, value):
        self._receiver_queue_name = value

    # ---------------------------------------------------------------------
    def __init__(self, l2_utils, sql_handler, data_logger_p=logging.getLogger(__name__)):
        # Initialize values

        global data_logger
        data_logger = data_logger_p

        data_logger.info(f"Subscriber initiated")

        self.l2_utils = l2_utils
        self.sql_handler = sql_handler

        self.connection_string = l2_utils.get_kv_secret("EGProcessQueueEndPoint").value
        self.receiver_queue_name = l2_utils.get_kv_secret("EGAggregationQueueName").value

        # self.init_servicebus(connection_string)

        # self.init_aggregators_receiver(receiver_queue_name)

    def init_servicebus(self):

        HTTP_PROXY = {
            'proxy_hostname': 'proxy-dmz.intel.com',  # proxy hostname.
            'proxy_port': 912  # proxy port.
        }
        self.servicebus_client = ServiceBusClient.from_connection_string(conn_str=self.connection_string,
                                                                         logging_enable=True, http_proxy=HTTP_PROXY)

    def init_aggregators_receiver(self):

        self.aggregators_receiver = self.servicebus_client.get_queue_receiver(queue_name=self.receiver_queue_name)

    def poll_messages(self):

        self.init_servicebus()

        with self.servicebus_client:
            self.init_aggregators_receiver()
            with self.init_aggregators_receiver:

                received_msgs = self.aggregators_receiver.receive_messages(max_message_count=1, max_wait_time=5)
                data_logger.warning(f"Received {len(received_msgs)} messages from the queue")

                renewer = AutoLockRenewer()

                for msg in received_msgs:
                    # automatically renew the lock on each message for 100 seconds
                    renewer.register(self.receiver, msg, max_lock_renewal_duration=100)
                data_logger.info("Register messages into AutoLockRenewer done.")

                for msg in received_msgs:
                    msg: azure.servicebus.ServiceBusMessage
                    data_logger.info(f"Message {msg.message_id} received")

                    stat = self.process_message(msg)
                    if stat == -1:
                        pass

                        # todo: need to decide what to do with failed messages
                        # it means that a file could not be parsed for some reason

                for msg in received_msgs:
                    self.complete_message(msg)

                renewer.close()

    def process_message(self, queue_msg):
        try:
            perf_start = perf_counter()
            sql_handler = AzureSqlHandler(self.l2_utils)

            msg = json.loads(str(queue_msg))
            data_logger.error(f"{msg['axon_id']} is being processed")

            result_dict = msg["header"]
            file_processes = msg["file_processes"]
            result_queue_name = msg["result_queue_name"]
            axon_id = msg["axon_id"]
            save_to_path = msg["save_to_path"]

            eg_queries_results = self.poll_queue_results(axon_id=axon_id,
                                                         file_processes=file_processes,
                                                         result_queue_name=result_queue_name)

            result_dict["eyeglass_queries"] = eg_queries_results

            sql_handler.WriteDataPipeStatus(axon_id, "Eyeglass pipe completed",
                                            perf_counter() - perf_start)

            data_logger.info(f"EyeGlass queries completed. elapsed : {str(perf_counter() - perf_start)}")

            result_json = json.dumps(result_dict, indent=4)

            adls_props = {"end_point": "ADLSAccountEndPoint",
                          "container": "ADLSAccountContainerTest",
                          "test_container": "ADLSAccountContainerTest"}

            adls_handler = ADLSHandler(self.l2_utils, adls_props)

            data_logger.info(f"adls_handler ready. elapsed : {str(perf_counter() - perf_start)}")
            sql_handler.WriteDataPipeStatus(axon_id, "ADLSHandler Initiated",
                                            perf_counter() - perf_start)

            adls_handler.create_directory(save_to_path)
            data_logger.info(f"ADLS directory created: {save_to_path}. elapsed : {str(perf_counter() - perf_start)}")

            adls_handler.create_file("{}.json".format(axon_id), result_json)
            data_logger.info(
                f"ADLS file created: {axon_id}. elapsed: {str(perf_counter() - perf_start)}")
            sql_handler.WriteDataPipeStatus(
                axon_id, "ADLS doc created",
                perf_counter() - perf_start)

            sql_handler.WriteDocToDB(axon_id, result_json)
            data_logger.info(
                f"DB file parsed: {axon_id}. elapsed: {str(perf_counter() - perf_start)}")

            sql_handler.WriteDataPipeStatus(axon_id, "Process completed",
                                            perf_counter() - perf_start)
            data_logger.info(
                f"EyeGlass document saved to db. elapsed: {str(perf_counter() - perf_start)}")

            adls_handler.delete_directory(axon_id)

        except Exception as exc:
            data_logger.error(f"{axon_id} generated an exception: {exc}")
            data_logger.error(f"Traceback: {traceback.format_exc()}")
            return -1

        else:
            data_logger.info(f"{axon_id} search completed successfully")

            return 0

    def complete_message(self, msg):
        self.aggregators_receiver.complete_message(msg)

    def empty_queue(self):

        received_msgs = self.aggregators_receiver.receive_messages(max_message_count=1000, max_wait_time=500000)
        for msg in received_msgs:
            self.aggregators_receiver.complete_message(msg)

    def send_a_message(self, msg):

        self.sender: azure.servicebus._servicebus_sender.ServiceBusSender
        message = ServiceBusMessage(json.dumps(msg))
        self.sender.send_messages(message)  # (message)

    def poll_queue_results(self, axon_id, file_processes, result_queue_name):

        perf_start = perf_counter()
        local_file_processes = file_processes
        all_results = []
        while len(local_file_processes) > 0:
            time.sleep(1)
            with self.servicebus_client.get_queue_receiver(queue_name=result_queue_name) as results_receiver:

                received_results = results_receiver.receive_messages(max_message_count=5, max_wait_time=5)
                data_logger.warning(f"Received {len(received_results)} messages from the queue")
                for msg in received_results:
                    msg: azure.servicebus.ServiceBusMessage
                    data_logger.info(f"Message {msg.message_id} received")
                    msg_content = json.loads(str(msg))

                    file_name = msg_content["file_name"]
                    all_results.extend(msg_content["query_results"])

                    del local_file_processes[file_name]

                    self.complete_message(msg)
                    data_logger.info(f"Pending processes: {len(local_file_processes)}")

        data_logger.info(f"EyeGlass results pulled from db. elapsed: {str(perf_counter() - perf_start)}")
        return all_results
