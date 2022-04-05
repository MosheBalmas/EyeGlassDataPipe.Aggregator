import azure.servicebus
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import json
import os

CONNECTION_STR = "Endpoint=sb://aiacdataengservicebusdev.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=uBLghbBvW7w5WXr1QjjgxVP3PXvhL0x9K1Az8PYpg+Y="
QUEUE_NAME = "aiac-dataeng-fileprocess-q"

os.environ["HTTP_PROXY"] = "http://proxy-dmz.intel.com:912"
os.environ["HTTPS_PROXY"] = "http://proxy-dmz.intel.com:912"

HTTP_PROXY = {
    'proxy_hostname': 'proxy-dmz.intel.com'  # proxy hostname.
    , 'proxy_port': 912  # proxy port.

}


def message_builder(file_process):
    queries_df = file_process["queries"]
    queries_json = queries_df.to_json(orient='records')[1:-1].replace('},{', '} {')
    process_id = file_process["axon_id"]
    process_file_name = file_process["file_name"]
    process_file_size = file_process["file_size"]

    msg = {"id=": process_id,
           "file_name": process_file_name,
           "file_size": process_file_size,
           "queries": queries_json

           }

    return ServiceBusMessage(json.dumps(msg))


def send_a_message(msg_list):
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True,
                                                                http_proxy=HTTP_PROXY)
    with servicebus_client:
        sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
        with sender:
            for msg in msg_list:
                sender: azure.servicebus._servicebus_sender.ServiceBusSender
                message = ServiceBusMessage(json.dumps(msg))
                sender.send_messages(message)  # (message)




