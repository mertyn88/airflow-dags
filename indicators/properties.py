base_path = '/home/airflow/airflow/dags'
#base_url = 'https://data-rest.lific.io/'
base_url = 'http://localhost:18095/'

insert_user = 'search'
insert_database = 'airflow'
insert_schema = 'data'
insert_port = 54322
insert_host = {
    'qa': 'qa-aurora-cluster.cluster-cfcpramzdlwi.ap-northeast-2.rds.amazonaws.com',
    'prod': 'acquisition-remaining.cluster-cfcpramzdlwi.ap-northeast-2.rds.amazonaws.com'
}
insert_password = {
    'qa': 'qa password',
    'prod': 'prod password'
}