from pydwdsti.db.base.DBConnector import DBConnector
from pydwdsti.programmability.sp.Procedure import Procedure

database = 'Survey_Sample_A19'
username = 'sa'
password = '.'
server = 'localhost'

qtAnswerColumn = 'COALESCE((SELECT a.Answer_Value FROM Answer as a WHERE a.UserId = u.UserId AND a.SurveyId = ' \
                 '<SURVEY_ID>AND a.QuestionId = <QUESTION_ID>), -1) AS ANS_Q<QUESTION_ID> '

qtNullColumn = ' NULL AS ANS_Q<QUESTION_ID> '

qtOuterUnionQuery = "SELECT UserId, <SURVEY_ID> as SurveyId, <DYNAMIC_QUESTION_ANSWERS> FROM [User] as u WHERE " \
                    "EXISTS ( SELECT * FROM Answer as a WHERE u.UserId = a.UserId AND a.SurveyId = <SURVEY_ID>) "

s = DBConnector(db_server=server, dbname=database, db_username=username, db_password=password)

s.selectBestDBDriverAvailable()
s.open()

sp = Procedure(qtAnswerColumn, qtNullColumn, qtOuterUnionQuery, s.conduit)
sp.startProcedure()

# rows = s.conduit.cursor().execute("select * from Question").fetchall()
#
# for r in rows:
#     print(r)
# if row:
#     print(row)
