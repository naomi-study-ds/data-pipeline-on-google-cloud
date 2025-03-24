from google.oauth2 import service_account
from google.cloud import bigquery

JSON_KEY_PATH = "/config/sprintda03_sa_key.json"

credentials = service_account.Credentials.from_service_account_file(JSON_KEY_PATH)
project_id = "probable-axon-451402-g1"
dataset = 'test'
table_name = 'accounts_userquestionrecord'
join_table_name = 'polls_question'
data_mart = 'dm_polls_interest'
location = 'asia-northeast3'

# 빅쿼리 클라이언트 정의 
client = bigquery.Client(
    project=project_id,
    location=location,
    credentials=credentials
)

# 조인쿼리 생성
df = client.query(
    query=f'''
            CREATE OR REPLACE TABLE {project_id}.{dataset}.{data_mart} AS
            SELECT 
                a.id,
                a.created_at,
                a.question_id,
                p.question_text
              FROM {project_id}.{dataset}.{table_name} AS a
              LEFT JOIN {project_id}.{dataset}.{join_table_name} AS p
              ON a.question_id = p.id
            ''',
    location=location
    )
