import sys
import traceback

from src.utils.L2Logger import L2Logger
from src.utils.PubSubHandler import PubSubHandler
from src.utils.L2_Utils import L2_Utils
from src.utils.AzureSqlHandler import AzureSqlHandler


def filter_environment_credential_warning(record):
    """
    This function does something
    """
    if record.name.startswith("azure.identity"):
        message = record.getMessage()
        return not message.startswith("EnvironmentCredential.get_token")
    return True


def poll(data_logger):

    while True:
        try:
            # Initialize logging

            run_mode = "Prod"
            l2_utils = L2_Utils()
            sql_handler = AzureSqlHandler(l2_utils)
            sub = PubSubHandler(l2_utils, sql_handler, run_mode=run_mode, data_logger_p=data_logger)
            sub.poll_db_messages()

        except Exception as e:

            data_logger.error("Exception of type %s occurred. Error: %s" % (e.__class__, str(e)))
            data_logger.error("Traceback: {}".format(traceback.format_exc()))
            sys.exit(f"Exception of type {e.__class__} occurred. Error: {str(e)} \n"
                     f"Traceback: {traceback.format_exc()}")

        finally:
            del l2_utils
            del data_logger
            del sql_handler
            del sub


def main():

    data_logger = L2Logger("EG.Aggregator", level="INFO").LOG
    poll(data_logger)


if __name__ == '__main__':
    main()
