from pydwdsti.db.base.DBConnector import DBConnector
from pydwdsti.programmability.sp.Procedure import Procedure
import os
from os import path
import subprocess
import sys

try:
    import pandas as pd
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'pandas'])
finally:
    import pandas as pd

try:
    import argparse as argp
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'argparse'])
finally:
    import argparse as argp

try:
    import pyodbc
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'pyodbc'])
finally:
    import pyodbc


def getSurveyStructure(conduit) -> pd.DataFrame:
    """
    The goal of this function is to return the survey structure as a pandas dataframe
    input : conduit from pyodbc
    output : pandas dataframe
    """
    surveyStructResults = conduit.cursor().execute("select * from SurveyStructure").fetchall()
    return pd.DataFrame(surveyStructResults)


def doesPersistenceFileExist(persistenceFilePath: str) -> bool:
    return path.exists(persistenceFilePath)


def isPersistenceFileDirectoryWritable(persistenceFilePath: str) -> bool:
    return os.access(persistenceFilePath, os.W_OK)


def refreshViewInDB(connector: DBConnector, baseViewQuery, viewName: str, persistenceFilePath: str) -> None:
    viewSurveyQuery = f"CREATE OR ALTER VIEW {viewName} AS "
    if connector.isDBConnectionOpen:
        viewCombinedQuery = f"{viewSurveyQuery}{baseViewQuery}"
        connector.conduit.cursor().execute(viewCombinedQuery)


def surveyResultsToDF(connector: DBConnector, viewName: str) -> pd.DataFrame:
    return pd.DataFrame(connector.conduit.cursor().execute(f"SELECT * FROM {viewName}").fetchall())


def getAllSurveyDataQuery(connector: DBConnector) -> str:
    procedure: Procedure = Procedure(connector.conduit)
    procedure.startProcedure()
    return procedure.finalQuery


def printSplashScreen():
    print("*************************************************************************************************")
    print("\t THIS SCRIPT ALLOWS TO EXTRACT SURVEY DATA FROM THE SAMPLE SEEN IN SQL CLASS")
    print("\t IT REPLICATES THE BEHAVIOUR OF A STORED PROCEDURE & TRIGGER IN A PROGRAMMATIC WAY")
    print("\t COMMAND LINE OPTIONS ARE:")
    print("\t\t -h or --help: print the help content on the console")
    print("*************************************************************************************************\n\n")


def processCLIArguments() -> dict:
    retParametersDictionary = None

    dbpassword: str = ''

    try:
        argParser: argp.ArgumentParser = argp.ArgumentParser(add_help=True)

        argParser.add_argument("-n", "--DSN", dest="dsn", action='store', default=None,
                               help="Sets the SQL Server DSN descriptor file - Take precedence over all access "
                                    "parameters",
                               type=str)
        argParser.add_argument("-server", dest="dbserver", help="sets the server identification", type=str)
        argParser.add_argument("-dbname", dest="dbname", help="sets the dbname", type=str)
        argParser.add_argument("-u", dest="dbusername", type=str)
        argParser.add_argument("-p", dest="dbuserpassword", type=str)
        argParser.add_argument("-t", dest="trustedmode", type=bool)
        argParser.add_argument("-v", dest="viewname", type=str, help="sets view name")
        argParser.add_argument("-pfp", dest="persistencefilepath", help="sets persistence file path")
        argParser.add_argument("-rfp", dest="resultsfilepath", help="sets results file path")

        argParsingResults = argParser.parse_args()

        retParametersDictionary = {
            "dsn": argParsingResults.dsn,
            "dbserver": argParsingResults.dbserver,
            "dbname": argParsingResults.dbname,
            "dbusername": argParsingResults.dbusername,
            "dbuserpassword": argParsingResults.dbuserpassword,
            "trustedmode": argParsingResults.trustedmode,
            "viewname": argParsingResults.viewname,
            "persistencefilepath": argParsingResults.persistencefilepath,
            "resultsfilepath": argParsingResults.resultsfilepath
        }

    except Exception as e:
        print("Command Line arguments processing error: " + str(e))

    return retParametersDictionary


def main():
    cliArguments: dict = None

    printSplashScreen()

    try:
        cliArguments = processCLIArguments()
    except Exception as excp:
        print("Exiting: " + str(excp))
        return

    if cliArguments is not None:
        """
        If using visual studio or pycharm the command line arguments should be set within the IDE
        """
        try:
            connector = DBConnector(db_server=cliArguments["dbserver"], dbname=cliArguments["dbname"],
                                    db_username=cliArguments["dbusername"], db_password=cliArguments["dbuserpassword"])

            connector.selectBestDBDriverAvailable()
            connector.open()

            surveyStructureDF: pd.DataFrame = getSurveyStructure(connector.conduit)
            baseViewQuery = getAllSurveyDataQuery(connector)

            if not doesPersistenceFileExist(cliArguments["persistencefilepath"]):
                refreshViewInDB(connector, baseViewQuery, cliArguments["viewname"], cliArguments["persistencefilepath"])
                if isPersistenceFileDirectoryWritable(cliArguments["persistencefilepath"]):
                    print("\nINFO - Content of SurveyResults table pickled in " + cliArguments[
                        "persistencefilepath"] + "\n")
                    surveyStructureDF.to_csv(cliArguments["persistencefilepath"])

            else:
                persistedSurveyStructureDF: pd.DataFrame = pd.read_csv(cliArguments["persistencefilepath"])
                if not surveyStructureDF.equals(persistedSurveyStructureDF):
                    refreshViewInDB(connector, getAllSurveyDataQuery(connector), cliArguments["viewname"],
                                    cliArguments["persistencefilepath"])

            surveyResultsToDF(connector, cliArguments["viewname"]).to_csv(cliArguments["resultsfilepath"])

            print("\nDONE - Results exported in " + cliArguments["resultsfilepath"] + "\n")

            connector.Close()

        except Exception as excp:
            print(excp)
    else:
        print("Inconsistency: CLI argument dictionary is None. Exiting")
        return


if __name__ == '__main__':
    main()
