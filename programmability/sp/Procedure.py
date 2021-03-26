from pydwdsti.utils.Utils import Utils


class Procedure:
    def __init__(self, conduit):
        self.ansQTAnswerColumn = """
                                COALESCE((SELECT a.Answer_Value 
                                FROM Answer as a WHERE a.UserId = u.UserId AND a.SurveyId = <SURVEY_ID>
                                AND a.QuestionId = <QUESTION_ID>), -1) AS ANS_Q<QUESTION_ID> """

        self.ansQTNullColumn = ' NULL AS ANS_Q<QUESTION_ID> '

        self.ansQTOuterUnionQuery = """SELECT UserId, <SURVEY_ID> as SurveyId, <DYNAMIC_QUESTION_ANSWERS> FROM [User] 
                                       as u WHERE EXISTS ( SELECT * FROM Answer as a WHERE u.UserId = a.UserId 
                                       AND a.SurveyId = <SURVEY_ID>)"""
        self.conduit = conduit
        self.currentUnionQueryBlock = ''
        self.finalQuery = ''

    def startProcedure(self):
        rows = self.conduit.cursor().execute("select SurveyId from Survey ORDER BY SurveyId").fetchall()
        for r, is_next in Utils.lookahead(rows):
            surveyId = str(r[0])
            currentQuestion = self.conduit.cursor().execute("SELECT * FROM ( SELECT SurveyId, QuestionId, 1 as "
                                                            "InSurvey FROM SurveyStructure WHERE SurveyId = " +
                                                            surveyId + "UNION SELECT " + surveyId + "as SurveyId, "
                                                                                                    "Q.QuestionId, "
                                                                                                    "0 as InSurvey "
                                                                                                    "FROM Question as "
                                                                                                    "Q WHERE NOT "
                                                                                                    "EXISTS (SELECT * "
                                                                                                    "FROM "
                                                                                                    "SurveyStructure "
                                                                                                    "as S WHERE "
                                                                                                    "S.SurveyId = " +
                                                            surveyId + "AND S.QuestionId = Q.QuestionId)) as t ORDER "
                                                                       "BY QuestionId")
            columnsQueryPart = ''
            for c, next in Utils.lookahead(currentQuestion):
                currentSurveyIdInQuestion, currentQuestionId, currentInSurvey = c

                if currentInSurvey == 0:
                    columnsQueryPart = columnsQueryPart + self.ansQTNullColumn.replace('<QUESTION_ID>',
                                                                                       str(currentQuestionId))
                else:
                    columnsQueryPart = columnsQueryPart + self.ansQTAnswerColumn.replace('<QUESTION_ID>',
                                                                                         str(currentQuestionId))
                if next:
                    columnsQueryPart = columnsQueryPart + ','

            self.ansQTOuterUnionQuery = self.ansQTOuterUnionQuery.replace('<DYNAMIC_QUESTION_ANSWERS>',
                                                                          columnsQueryPart)
            self.ansQTOuterUnionQuery = self.ansQTOuterUnionQuery.replace('<SURVEY_ID>', surveyId)

            self.finalQuery = self.finalQuery + self.ansQTOuterUnionQuery

            if is_next:
                self.finalQuery = self.finalQuery + ' UNION '

    @property
    def getFinalQuery(self):
        return self.finalQuery
