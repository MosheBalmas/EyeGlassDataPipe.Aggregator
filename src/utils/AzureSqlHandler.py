import logging
import pyodbc

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
            "AzureSqlServer").value  # 'aiacdataengsqldev.privatelink.AzureSqlServerDb.windows.net'
        self.AzureSqlServerPort = l2_utils.get_kv_secret("AzureSqlServerPort").value  # '1433'
        self.AzureSqlServerDb = l2_utils.get_kv_secret("AzureSqlServerDb").value  # 'repol2'
        self.AzureSqlServerUser = l2_utils.get_kv_secret("AzureSqlServerUser").value  # 'laasdataengadmin'
        self.AzureSqlServerPass = l2_utils.get_kv_secret("AzureSqlServerPass").value  # '1@AzureSqlServerPass123'
        self.AzureSqlServerDriver = l2_utils.get_kv_secret(
            "AzureSqlServerDriver").value  # '{ODBC AzureSqlServerDriver 17 for SQL AzureSqlServer}'

        self.connection_string = 'DRIVER=' + self.AzureSqlServerDriver + ';SERVER=tcp:' + self.AzureSqlServer + ',' + self.AzureSqlServerPort + ';DATABASE=' + self.AzureSqlServerDb + ';UID=' + self.AzureSqlServerUser + ';PWD=' + self.AzureSqlServerPass

    def __str__(self):
        return self.connection_string


    def WriteProcessStatusToDB(self, pid, status, msg):
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            sql = """\
            EXEC Data_Utils.UpdateProcessStatus @pid=?, @proc_status=?, @proc_msg=?
            """
            params = (pid, status, msg)
            cursor.execute(sql, params)
