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
    def aggregators_sender(self):
        return self._aggregators_sender

    @aggregators_sender.setter
    def aggregators_sender(self, value):
        self._aggregators_sender = value

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

    @property
    def run_mode(self):
        return self._run_mode

    @run_mode.setter
    def run_mode(self, value):
        self._run_mode = value

    # ---------------------------------------------------------------------
    def __init__(self, l2_utils, sql_handler, run_mode="Prod", data_logger_p=logging.getLogger(__name__)):
        # Initialize values

        global data_logger
        data_logger = data_logger_p

        data_logger.info(f"Subscriber initiated")

        self.l2_utils = l2_utils
        self.sql_handler = sql_handler
        self.run_mode = run_mode
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

    def init_aggregators_sender(self):
        self.servicebus_client: azure.servicebus.ServiceBusClient

        self.aggregators_sender = self.servicebus_client.get_queue_sender(queue_name=self.receiver_queue_name)

    def poll_messages(self):

        self.init_servicebus()

        with self.servicebus_client:
            self.init_aggregators_receiver()
            with self.aggregators_receiver:

                received_msgs = self.aggregators_receiver.receive_messages(max_message_count=1, max_wait_time=5)
                data_logger.warning(f"Received {len(received_msgs)} messages from the queue")

                if len(received_msgs) > 0:
                    renewer = AutoLockRenewer()

                    for msg in received_msgs:
                        # automatically renew the lock on each message for 100 seconds
                        renewer.register(self.aggregators_receiver, msg, max_lock_renewal_duration=100)
                    data_logger.info("Register messages into AutoLockRenewer done.")

                    for msg in received_msgs:
                        msg: azure.servicebus.ServiceBusMessage
                        data_logger.info(f"Aggregator message {msg.message_id} received")

                        response = self.process_message(msg)
                        if response["status"] == -1:
                            data_logger.info(f"Aggregator process {msg.message_id} failed")

                            # todo: need to decide what to do with failed messages
                            # it means that a file could not be parsed for some reason
                        else:
                            for msg in received_msgs:
                                self.complete_results_messages(msg_list=response["handled_messages"],
                                                               result_queue_name=response["result_queue_name"])
                                data_logger.info(f"Message {msg.message_id} results marked as completed")

                                self.complete_aggregator_message(msg)
                                data_logger.info(f"Message {msg.message_id} completed")

                                if response["requeue_message_content"] is not None:
                                    self.requeue_agg_message(response["requeue_message_content"])
                                    data_logger.info(f"Message {msg.message_id} re-queued")

                    renewer.close()

    def process_message(self, queue_msg):
        try:
            perf_start = perf_counter()
            sql_handler = AzureSqlHandler(self.l2_utils)

            msg = json.loads(str(queue_msg))
            data_logger.info(f"{msg['axon_id']} is being processed")

            result_dict = msg["header"]
            file_processes = msg["file_processes"]
            result_queue_name = msg["result_queue_name"]
            axon_id = msg["axon_id"]
            save_to_path = msg["save_to_path"]
            run_mode = msg["run_mode"]

            eg_queries_results = self.process_current_results(axon_id=axon_id,
                                                              file_processes=file_processes,
                                                              result_queue_name=result_queue_name)

            handled_messages = eg_queries_results["handled_messages"]

            result_dict["eyeglass_queries"].extend(eg_queries_results["new_results"])
            msg_to_requeue = {}

            if len(eg_queries_results["new_file_processes"]) > 0:
                # if we did not receive all results from results queue
                # we need to update the message and requeue

                result_dict["eyeglass_queries"].extend(eg_queries_results["new_results"])
                msg_to_requeue["header"] = result_dict
                msg_to_requeue["file_processes"] = eg_queries_results["new_file_processes"]
                msg_to_requeue["axon_id"] = axon_id
                msg_to_requeue["run_mode"] = run_mode
                msg_to_requeue["save_to_path"] = save_to_path
                msg_to_requeue["result_queue_name"] = result_queue_name

            else:
                # if all results collected, store the final document in the ADLS
                sql_handler.WriteDataPipeStatus(axon_id, "Eyeglass pipe completed",
                                                perf_counter() - perf_start)

                data_logger.info(f"EyeGlass queries completed. elapsed : {str(perf_counter() - perf_start)}")

                result_json = json.dumps(result_dict, indent=4)

                adls_props = {"end_point": "ADLSAccountEndPoint",
                              "container": "ADLSAccountContainerTest",
                              "test_container": "ADLSAccountContainerTest"}

                adls_handler = ADLSHandler(l2_utils=self.l2_utils, run_mode=self.run_mode,  adls_props=adls_props)

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

                # sql_handler.WriteDocToDB(axon_id, result_json)
                # data_logger.info(
                #     f"DB file parsed: {axon_id}. elapsed: {str(perf_counter() - perf_start)}")

                sql_handler.WriteDataPipeStatus(axon_id, "Process completed",
                                                perf_counter() - perf_start)
                # data_logger.info(
                #     f"EyeGlass document saved to db. elapsed: {str(perf_counter() - perf_start)}")

                adls_process_files_props = {"end_point": "FileProcessStorageAccountEndPoint",
                                            "container": "FileProcessStorageContainer",
                                            "test_container": "FileProcessStorageContainerTest"}

                adls_process_files = ADLSHandler(l2_utils=self.l2_utils, run_mode=self.run_mode, adls_props=adls_process_files_props)

                adls_process_files.delete_directory(axon_id)

            data_logger.info(f"{axon_id} search completed successfully")
            return {"status": 0,
                    "handled_messages": handled_messages,
                    "result_queue_name": result_queue_name,
                    "requeue_message_content": msg_to_requeue if len(eg_queries_results["new_file_processes"]) > 0 else None
                    }

        except Exception as exc:
            data_logger.error(f"{axon_id} generated an exception: {exc}")
            data_logger.error(f"Traceback: {traceback.format_exc()}")
            return {"status": -1,
                    "handled_messages": None,
                    "result_queue_name": result_queue_name,
                    "requeue_message_content": None
                    }

    def requeue_agg_message(self, msg):

        self.init_aggregators_sender()
        self.sender: azure.servicebus._servicebus_sender.ServiceBusSender
        message = ServiceBusMessage(json.dumps(msg))
        self.aggregators_sender.send_messages(message)  # (message)

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

                received_results = results_receiver.receive_messages(max_message_count=10, max_wait_time=5)
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

    def process_current_results(self, axon_id, file_processes, result_queue_name):

        perf_start = perf_counter()
        local_file_processes = file_processes
        all_results = []

        with self.servicebus_client.get_queue_receiver(queue_name=result_queue_name) as results_receiver:

            received_results = results_receiver.receive_messages(max_message_count=5, max_wait_time=5)
            data_logger.warning(f"Received {len(received_results)} result messages from the queue")

            # renewer = AutoLockRenewer()
            #
            # for msg in received_results:
            #     # automatically renew the lock on each message for 100 seconds
            #     renewer.register(results_receiver, msg, max_lock_renewal_duration=100)
            # data_logger.info("Register messages into AutoLockRenewer done.")

            for msg in received_results:
                msg: azure.servicebus.ServiceBusMessage
                data_logger.info(f"Message {msg.message_id} received")
                msg_content = json.loads(str(msg))

                file_name = msg_content["file_name"]

                current_result_set = {"query_status": msg_content["query_status"],
                                      "query_message": msg_content["query_mgs"],
                                      "query_results": msg_content["query_results"],
                                      }

                all_results.extend(current_result_set)

                del local_file_processes[file_name]

                # self.complete_message(msg)
                data_logger.info(f"Pending processes: {len(local_file_processes)}")

            # for msg in received_results:
            #     self.complete_message(msg)

        data_logger.info(f"EyeGlass results pulled from db. elapsed: {str(perf_counter() - perf_start)}")
        return {"new_results": all_results,
                "new_file_processes": local_file_processes,
                "handled_messages": received_results}

    def complete_aggregator_message(self, msg):
        self.aggregators_receiver.complete_message(msg)

    def complete_results_messages(self, msg_list,result_queue_name):
        with self.servicebus_client.get_queue_receiver(queue_name=result_queue_name) as results_receiver:
            for msg in msg_list:
                results_receiver.complete_message(msg)

    def empty_queue(self):

        received_msgs = self.aggregators_receiver.receive_messages(max_message_count=1000, max_wait_time=500000)
        for msg in received_msgs:
            self.aggregators_receiver.complete_message(msg)

