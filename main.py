from src.utils.L2Logger import L2Logger
from src.utils.Pub_Sub_Handler import Pub_Sub_Handler
from src.utils.L2_Utils import L2_Utils
from src.utils.AzureSqlHandler import AzureSqlHandler
import logging
import sys
import traceback


def filter_environment_credential_warning(record):
    """
    This function does something
    """
    if record.name.startswith("azure.identity"):
        message = record.getMessage()
        return not message.startswith("EnvironmentCredential.get_token")
    return True


def main():
    try:
        # Initialize logging
        data_logger = L2Logger("ProcessEGFile", level="WARNING").LOG

        l2_utils = L2_Utils()
        sql_handler = AzureSqlHandler(l2_utils)
        sub = Pub_Sub_Handler(l2_utils, sql_handler)

        while True:
            sub.poll_messages()

    except Exception as e:

        data_logger.error("Exception of type %s occurred. Error: %s" % (e.__class__, str(e)))
        data_logger.error("Traceback: {}".format(traceback.format_exc()))

    finally:
        del l2_utils
        del data_logger
        del sql_handler
        del sub

if __name__ == '__main__':
    main()
