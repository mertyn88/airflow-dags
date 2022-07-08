s3_normal_key = 'normal_key'
s3_secret_key = 'secret_key'
s3_bucket = 'amplitude-event'
s3_prefix = '{file}/{year}/{month}/{day}.ndjson'
s3_region = 'ap-northeast-2'

hdfs_url = 'http://{host}:9870'
hdfs_user = 'hdfs'
hdfs_copy_path  = '/amplitude-event/{file}/year={year}/month={month}'

amplitude_url = 'https://amplitude.com/api/2/export?start={date}T00&end={date}T23'
amplitude_auth_key = 'auth_key'

base_path = '/home/airflow/airflow/dags'