import logging
import pyodbc
import json
import time
from time import perf_counter

data_logger = logging.getLogger(__name__)

class AzureSqlHandler:

    @property
    def AzureSqlServer(self):
        return self._AzureSqlServer

    @AzureSqlServer.setter
    def AzureSqlServer(self, value):
        self._AzureSqlServer = value

    @property
    def AzureSqlServerPort(self):
        return self._AzureSqlServerPort

    @AzureSqlServerPort.setter
    def AzureSqlServerPort(self, value):
        self._AzureSqlServerPort = value

    @property
    def AzureSqlServerDb(self):
        return self._AzureSqlServerDb

    @AzureSqlServerDb.setter
    def AzureSqlServerDb(self, value):
        self._AzureSqlServerDb = value

    @property
    def AzureSqlServerUser(self):
        return self._AzureSqlServerUser

    @AzureSqlServerUser.setter
    def AzureSqlServerUser(self, value):
        self._AzureSqlServerUser = value

    @property
    def AzureSqlServerPass(self):
        return self._AzureSqlServerPass

    @AzureSqlServerPass.setter
    def AzureSqlServerPass(self, value):
        self._AzureSqlServerPass = value

    @property
    def AzureSqlServerDriver(self):
        return self._AzureSqlServerDriver

    @AzureSqlServerDriver.setter
    def AzureSqlServerDriver(self, value):
        self._AzureSqlServerDriver = value

    @property
    def connection_string(self):
        return self._connection_string

    @connection_string.setter
    def connection_string(self, value):
        self._connection_string = value

    def __init__(self, l2_utils):
        self.AzureSqlServer = l2_utils.get_kv_secret(
            "AzureSqlServer").value
        self.AzureSqlServerPort = l2_utils.get_kv_secret("AzureSqlServerPort").value
        self.AzureSqlServerDb = l2_utils.get_kv_secret("AzureSqlServerDb").value
        self.AzureSqlServerUser = l2_utils.get_kv_secret("AzureSqlServerUser").value
        self.AzureSqlServerPass = l2_utils.get_kv_secret("AzureSqlServerPass").value
        self.AzureSqlServerDriver = l2_utils.get_kv_secret(
            "AzureSqlServerDriver").value  # '{ODBC AzureSqlServerDriver 17 for SQL AzureSqlServer}'

        self.connection_string = 'DRIVER=' + self.AzureSqlServerDriver + ';SERVER=tcp:' + self.AzureSqlServer + ',' + self.AzureSqlServerPort + ';DATABASE=' + self.AzureSqlServerDb + ';UID=' + self.AzureSqlServerUser + ';PWD=' + self.AzureSqlServerPass

    def __str__(self):
        return self.connection_string


    def WriteProcessStatusToDB(self, pid, status, msg, proc_result):
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            sql = """\
            EXEC Data_Utils.UpdateProcessStatus @pid=?,@proc_status=?,@proc_msg=?,@proc_result=?
            """
            # params = (pid,status,msg,','.join(json.dumps(res) for res in proc_result))
            params = (pid, status, msg, str(proc_result))
            cursor.execute(sql, params)

    def poll_results(self, axon_id):

        perf_start = perf_counter()
        pending_results = self.azure_sql_handler.GetPendingResultsList(axon_id)
        # total_processes = pending_results[0]
        total_pending = pending_results[1]
        # total_pass = pending_results[2]
        # total_fail = pending_results[3]

        while total_pending > 0:
            time.sleep(5)
            data_logger.info(f"Pending processes: {total_pending}")

            pending_results = self.azure_sql_handler.GetPendingResultsList(axon_id)
            total_pending = pending_results[1]

        all_results = self.azure_sql_handler.GetResults(axon_id)

        data_logger.info(f"EyeGlass results pulled from db. elapsed: {str(perf_counter() - perf_start)}")
        return all_results
