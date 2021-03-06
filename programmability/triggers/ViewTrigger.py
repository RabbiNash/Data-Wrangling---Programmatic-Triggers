class ViewTrigger:
    def __init__(self, view_query, db_conduit):
        self.view_query = view_query
        self._sql_survey_data = 'CREATE OR ALTER VIEW vw_allSurveyData AS '
        self.db_conduit = db_conduit

    def create_or_alter_view(self):
        self.db_conduit.cursor().execute(self._sql_survey_data + self.view_query)

    def retrieve_view(self):
        return self.db_conduit.cursor.execute("SELECT * FROM vw_allSurveyData")