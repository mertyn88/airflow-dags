from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook
from dateutil.relativedelta import relativedelta


class SlackAlert:
    def __init__(self, channel):
        self.channel = channel

    def on_failure(self, context):
        dag_id = context.get('task_instance').dag_id
        task_id = context.get('task_instance').task_id
        execution_date = (context.get('execution_date') + relativedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S')
        next_execution_date = (context.get('next_execution_date') + relativedelta(hours=9)).strftime(
            '%Y-%m-%d %H:%M:%S')
        log_url = context.get('task_instance').log_url

        alert = SlackWebhookHook(
            http_conn_id="slack_conn",
            channel=self.channel,
            username='airflow_bot',
            message=f"""
            *[:exclamation: AIRFLOW ERROR REPORT]*
            ■ DAG: _{dag_id}_
            ■ Task: _{task_id}_
            ■ Execution Date (KST): _{execution_date}_
            ■ Next Execution Date (KST): _{next_execution_date}_
            ■ Log URL: {log_url}
            """
        )
        return alert.execute()
