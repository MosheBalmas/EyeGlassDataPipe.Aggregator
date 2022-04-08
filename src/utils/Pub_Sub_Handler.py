import json
import traceback
import logging

import azure.servicebus
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from src.utils.L2_Utils import exec_single_matched_file

# os.environ["HTTP_PROXY"] = "http://proxy-dmz.intel.com:912"
# os.environ["HTTPS_PROXY"] = "http://proxy-dmz.intel.com:912"

data_logger = logging.getLogger(__name__)




class Pub_Sub_Handler:

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
    def receiver(self):
        return self._receiver

    @receiver.setter
    def receiver(self, value):
        self._receiver = value

    @property
    def sender(self):
        return self._sender

    @sender.setter
    def sender(self, value):
        self._sender = value

    # ---------------------------------------------------------------------
    def __init__(self, l2_utils, sql_handler):
        # Initialize values
        data_logger.info(f"Subscriber initiated")

        self.l2_utils = l2_utils
        self.sql_handler = sql_handler
        
        connection_string = l2_utils.get_kv_secret("EGProcessQueueEndPoint").value
        receiver_queue_name = l2_utils.get_kv_secret("EGProcessQueueName").value
        sender_queue_name = l2_utils.get_kv_secret("EGProcessResultsQueueName").value

        self.init_servicebus(connection_string)

        self.init_receiver(connection_string, receiver_queue_name)
        self.init_sender(connection_string, sender_queue_name)

    def init_servicebus(self, connection_string):

        HTTP_PROXY = {
            'proxy_hostname': 'proxy-dmz.intel.com',  # proxy hostname.
            'proxy_port': 912  # proxy port.
        }
        self.servicebus_client = ServiceBusClient.from_connection_string(conn_str=connection_string,
                                                                         logging_enable=True, http_proxy=HTTP_PROXY)

    def init_receiver(self, connection_string, queue_name):

        self.receiver = self.servicebus_client.get_queue_receiver(queue_name=queue_name)

    def init_sender(self, connection_string, queue_name):
        self.servicebus_client: azure.servicebus.ServiceBusClient

        self.sender = self.servicebus_client.get_queue_sender(queue_name=queue_name)

    def poll_messages(self):
        received_msgs = self.receiver.receive_messages(max_message_count=5, max_wait_time=5)
        data_logger.warning(f"Received {len(received_msgs)} messages from the queue")
        for msg in received_msgs:
            msg: azure.servicebus.ServiceBusMessage
            data_logger.info(f"Message {msg.message_id} received")

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
            data_logger.error(f"{msg['pid']} is being processed")
            query_results = exec_single_matched_file(msg["file_name"], msg["file_size"], msg["queries"],
                                                     msg["axon_id"])

        except Exception as exc:
            data_logger.error(f"{msg['file_name']} generated an exception: {exc}")
            data_logger.error(f"Traceback: {traceback.format_exc()}")
            self.sql_handler.WriteProcessStatusToDB(msg["pid"],"fail", traceback.format_exc(), None)
        else:
            data_logger.info(f"{msg['file_name']} search completed successfully")
            self.send_a_message(query_results)

            self.sql_handler.WriteProcessStatusToDB(msg["pid"], "pass", "", query_results)
            return 0

    def complete_message(self, msg):
        self.receiver.complete_message(msg)

    def empty_queue(self):

        received_msgs = self.receiver.receive_messages(max_message_count=1000, max_wait_time=500000)
        for msg in received_msgs:
            self.receiver.complete_message(msg)

    def send_a_message(self, msg):

        self.sender: azure.servicebus._servicebus_sender.ServiceBusSender
        message = ServiceBusMessage(json.dumps(msg))
        self.sender.send_messages(message)  # (message)



