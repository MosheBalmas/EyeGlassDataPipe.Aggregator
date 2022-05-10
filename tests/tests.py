from src.utils.L2Logger import L2Logger
from src.utils.L2_Utils import L2_Utils
from src.utils.ADLSHandler import ADLSHandler
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


def test_delete_storage_files(l2_utils, axon_id):
    adls_props = {"end_point": "FileProcessStorageAccountEndPoint",
                  "container": "FileProcessStorageContainer",
                  "test_container": "FileProcessStorageContainerTest"}

    adls_handler = ADLSHandler(l2_utils=l2_utils, run_mode="Test", adls_props=adls_props)

    adls_handler.delete_directory(axon_id)

def main():
    try:
        # Initialize logging
        # data_logger = L2Logger("EG.Aggregator.tests", level="INFO").LOG
        axon_id = "185753c0-1dee-4d0e-af73-4d981a7f667d"
        l2_utils = L2_Utils()
        test_delete_storage_files(l2_utils, axon_id)

    except Exception as e:

        print("Exception of type %s occurred. Error: %s" % (e.__class__, str(e)))
        print("Traceback: {}".format(traceback.format_exc()))

    finally:
        del l2_utils



if __name__ == '__main__':
    main()
