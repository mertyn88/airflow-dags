# dags
Python airflow dags


## Amplitude
amplitude에 적재되는 데이터를 AWS S3와 hdfs에 저장한다. 이때, 이벤트 별로 분리하여 별도의 .ndjson형태로 저장한다.  
example 폴더 예제 파일 참조

## Indicators
data-rest의 지표 정보를 받아 RDB에 저장한다.

```
[2022-07-08 10:11:23,982] [INFO] [job_event.py:44] [run] [http://localhost:18095//partner/branch/import_approve_category/airflow/2022-07-07/2022-07-07]
[2022-07-08 10:11:24,884] [INFO] [database.py:24] [insert_data] [Execute query columns ::: INSERT INTO event_category_daily(minor_category_id, minor_category_name, count, event_code, range_at) VALUES %s]
[2022-07-08 10:11:25,081] [INFO] [database.py:40] [insert_data] [The dataframe is inserted]
[2022-07-08 10:11:25,082] [INFO] [job_event.py:44] [run] [http://localhost:18095//partner/branch/import_approve_location/airflow/2022-07-07/2022-07-07]
[2022-07-08 10:11:25,434] [INFO] [database.py:24] [insert_data] [Execute query columns ::: INSERT INTO event_location_daily(location_city, count, event_code, range_at) VALUES %s]
```
