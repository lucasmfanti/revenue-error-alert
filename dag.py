import requests
import json
import pandas as pd
import datetime as dt
from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def revenue_error():
    
    now = str((datetime.now() - timedelta(hours = 3)).replace(microsecond = 0))

    query = f'''
        SELECT 
            CONCAT('Event: ', CAST(rv.event_id AS STRING)) AS event_id,
            CONCAT('Value: ', CAST(rv.value AS STRING)) AS value,
            CONCAT('Customer: ', RPAD(cu.name, 20)) AS customer
        FROM `database.revenues` rv
        LEFT JOIN `database.events` ev
            ON rv.event_id = ev.id
        INNER JOIN `database.customers` cu
            ON ev.customer_id = cu.id
        WHERE TRUE
            AND value_class = 'Revenue'
            AND value < 1
            AND DATETIME(rv.created_at) > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 4 HOUR)
        ORDER BY event_id ASC
        '''

    bq = BigQuery()

    df = bq.query(query)

    x = df.to_json(orient = 'split' , index=False)
    d = json.loads(x)
    d = d["data"]
    d = str(d)
    d = d.replace("[", "")
    d = d.replace("],","\n\n")
    d = d.replace("]", "")
    d = d.replace("'","")

    webhook = 'https://hooks.slack.com/services/##########################################'
    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Revenue Error! :alert:"
                }
            },
            
            {
                "type": "context",
                "elements": [
                    {
                        "text": f"{now}",
                        "type": "mrkdwn"
                    }
                ]
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f":alert: Events :alert:\n\n {d}\n\n  "
                }
            },
        ]
    }

    if len(df) > 0:
        requests.post(webhook, json.dumps(payload))
    else:
        return 'empty payload'

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 1, 1, 0, 0, 0),
    'concurrency': 1,
    'email': 'youremail@email.com.br',
    'email_on_failure': True,
    'email_on_retry': False
}

with DAG('revenue_error_dag',
         default_args=default_args,
         schedule_interval='0 */4 * * 1-5',
         catchup=False
         ) as dag:

    revenue_error = PythonOperator(task_id = 'revenue_error', python_callable = revenue_error)