# Standard Python does not provide abstract classes, but module abc in standard library
from abc import ABC, abstractmethod
import pyodbc


class BaseDBConnector(ABC):
    def __init__(self: object, db_server: str = None, dbname: str = None, db_username: str = None, \
                 db_password: str = None):

        self.db_server = db_server
        self.dbname = dbname
        self.db_username = db_username
        self.dbpassword = db_password

        self.isDBConnectionOpen: bool = False
        self.conduit: pyodbc.Connection = None
        self.driver: str = 'undef'

    # Decorator @abstractmethod from ABC package
    @abstractmethod
    def selectBestDBDriverAvailable(self: object) -> None:
        pass

    # Method "Open", to create the data source connection via the conduit object
    def open(self: object):
        # Don't reopen a connection, if the it's already
        # This should be using a property rather that the _isDBConnectionOpen attribute
        if not self.isDBConnectionOpen:
            if self.conduit is None:  # Use a property. Logic: if the pyodbc object is not yet instanciated
                # Ok, create the conduit
                # This method will only work in a child class, as the value of self._driver will be set by the
                # implemented pure virtual / abstract method selectBestDBDriverAvailable

                # TODO: add some tests to choose between connecting using separated parameters (see below)
                #   or using the DSN attribute

                # The method pyodbc.connect creates an Input/Output (networked I/O)
                # It needs to be exception-managed
                try:
                    # Is it the right location to call the abstract method?
                    # No, because it has no code in DBConnector, purely virtual
                    # It has to be called in a child class, with a forced implementation
                    # selectBestDBDriverAvailable()
                    self.conduit = pyodbc.connect("DRIVER=" + self.driver \
                                                   + ";SERVER=" + self.db_server + ";DATABASE=" + self.dbname + ";UID=" + self.db_username + ";PWD=" + self.dbpassword, autocommit=True)

                    self.isDBConnectionOpen = True

                except Exception as excp:
                    self.isDBConnectionOpen = False
                    print(self.dbpassword)
                    raise Exception("Couldn't connect to the database").with_traceback(excp.__traceback__)
                    # Capture all exceptions (using the base class Exception)
                    # Creation of an anonymous instance of the Exception class and "raising" / throwing the
                    # exception to caller

            else:  # if(self._conduit is None)
                # The flag indicates an closed connection but the conduit object (pyodbc instance) is not None
                # The DBConnector object is in inconsistent state
                raise Exception(
                    "Inconsistent state of the connector, flag set to not connected, conduit is not None")
        else:
            # else of if(self._isDBConnectionOpen == False)
            # The connection is already open, improper use of the Open() method
            # We should raise an exception, it's a programmer's mistake
            raise Exception("Inconsistent call to Open(), the connector is already connected to the data source")

    def Close(self: object) -> None:
        if self.isDBConnectionOpen:
            if self.conduit is not None:

                try:
                    self.conduit.close()

                    self.isDBConnectionOpen = False
                    self.conduit = None

                except Exception as excp:
                    raise Exception('Couldn''t close the DB connection').with_traceback(excp.__traceback__)
            else:
                raise Exception(
                    'Internal DBConnector object inconsistency - Internal flag says ''Connected'' but pyodbc Connector is none')

    @property
    def dbServer(self: object) -> str:
        return self._db_server

    @dbServer.setter
    def dbServer(self: object,
                 value: str) -> None:  # example of a type hint of return type to None --> means that it's a procedure
        self.db_server = value

    @property
    def dbDriver(self: object):
        return self.__driver

    @dbDriver.setter
    def dbDriver(self: object,
                 value: str) -> None:  # example of a type hint of return type to None --> means that it's a procedure
        self.driver = value

    @property
    def dbConduit(self: object):
        return self.__conduit