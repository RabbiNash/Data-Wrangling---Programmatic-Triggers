from BaseDBConnector import BaseDBConnector
import pyodbc


class DBConnector(BaseDBConnector):
    _instance = None

    def __init__(self, db_server: str = None, dbname: str = None, db_username: str = None, \
                 db_password: str = None):
        self.conduit = None
        if DBConnector._instance is None:
            super().__init__(db_server, dbname, db_username, db_password)
        else:
            DBConnector._instance = self

    @property
    def instance(self):
        return self._instance

    def open(self):
        super().open()

    @staticmethod
    def getInstance(db_server: str = None, dbname: str = None, db_username: str = None, db_password: str = None):
        if DBConnector.instance is None:
            DBConnector._instance = DBConnector(db_server, dbname, db_username, db_password)
        return DBConnector.instance

    def selectBestDBDriverAvailable(self: object) -> None:
        drivers = [driver for driver in pyodbc.drivers()]
        print(drivers)
        self.dbDriver = drivers[0]
