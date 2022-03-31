from src.utils.L2Logger import L2Logger
from src.utils.subscriber import Subscriber
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
        data_logger = L2Logger("ProcessEGFile", level="INFO").LOG

        handler = logging.StreamHandler(sys.stdout)

        handler.addFilter(filter_environment_credential_warning)

        data_logger.addHandler(handler)

        sub = Subscriber(data_logger)
        while True:
            sub.poll_messages()

    except Exception as e:

        data_logger.error("Exception of type %s occurred. Error: %s" % (e.__class__, str(e)))
        data_logger.error("Traceback: {}".format(traceback.format_exc()))
    finally:

        for h in data_logger.handlers:
            data_logger.removeHandler(h)

        del sub

if __name__ == '__main__':
    main()
