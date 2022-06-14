import time
import logging
import logging.config
import sys


class L2Logger:

    @property
    def LOG(self):
        return self._LOG

    @LOG.setter
    def LOG(self, value):
        self._LOG = value

    @property
    def log_file_name(self):
        return self._log_file_name

    @log_file_name.setter
    def log_file_name(self, value):
        self._log_file_name = value

    @property
    def write_mode(self):
        return self._write_mode

    @write_mode.setter
    def write_mode(self, value):
        self._write_mode = value

    @property
    def proc_name(self):
        return self._proc_name

    @proc_name.setter
    def proc_name(self, value):
        self._proc_name = value

    @property
    def level(self):
        return self._level

    @level.setter
    def level(self, value):
        self._level = value

    def __init__(self, proc, write_mode="a", level="INFO"):
        self.log_file_name = "%s_%s.log" % (proc, time.strftime("%Y%m%d-%H"))
        self.write_mode = write_mode or "w"
        self.level = level = level or "INFO"
        self.init_logging()
        self.LOG.info(f"Logger is {self.log_file_name}")

    def init_logging(self):
        DEFAULT_LOGGING = {
            "version": 1,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(module)s:  %(message)s",
                    "datefmt": "%d/%m/%Y %I:%M:%S %p"},
            },
            "handlers": {
                "console": {"class": "logging.StreamHandler",
                            "formatter": "standard",
                            "level": "INFO",
                            "stream": sys.stdout},
                "file": {"class": "logging.FileHandler",
                         "formatter": "standard",
                         "level": "INFO",
                         "filename": f"Logs/{self.log_file_name}", "mode": self.write_mode}
            },
            "loggers": {
                __name__: {"level": "INFO",
                           "handlers": ["console", "file"],
                           "propagate": False},
            }
        }

        logging.config.dictConfig(DEFAULT_LOGGING)

        # logging.basicConfig(level=self.level,
        #                     format="%(asctime)s %(levelname)s %(threadName)s %(name)s %(message)s",
        #                     datefmt="%d/%m/%Y %I:%M:%S %p",
        #                     filename=f"Logs/{self.log_file_name}.%s" ,
        #                     filemode=self.write_mode)

        # global LOG
        self.LOG = logging.getLogger(__name__)
