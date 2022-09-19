# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 15:30:49 2022

@author: Walter Ferreira dos Santos
"""

from datetime import datetime, timedelta  
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
##Possível enviar no Teams quando a carga falhar
##from ms_teams_webhook_operator import MSTeamsWebhookOperator
import pendulum

local_tz = pendulum.timezone("Brazil/East")

def on_success(context):

    dag_id = context['dag_run'].dag_id
    task_id = context['task_instance'].task_id
    duration_time = context['task_instance'].duration
    try_number = context['task_instance']._try_number

    context['task_instance'].xcom_push(key=dag_id, value=True)        

    #logs_url = "http://10.0.10.178/log?dag_id={}&task_id={}&execution_date={}".format(dag_id, task_id, context['ts'].split("+")[0])
    ##Possível enviar no Teams quando a carga falhar
    # teams_notification = MSTeamsWebhookOperator(
    #     task_id="msteams_notify_failure", trigger_rule="all_done",
    #     message="Task `{}` da DAG `{}` executou com sucesso.<br>Duracao:  `{}` segs. Tentativa(s): `{}`. ".format(task_id,dag_id,duration_time,try_number),
    #     button_text="Veja o Log", button_url=logs_url,
    #     theme_color="00FF00", http_conn_id='webhook-ok-airflow')
		
    #teams_notification.execute(context)
	
	
def on_failure(context):

    dag_id = context['dag_run'].dag_id
    task_id = context['task_instance'].task_id
    duration_time = context['task_instance'].duration
    owner_name = context['task'].owner

    context['task_instance'].xcom_push(key=dag_id, value=True)
	
    #logs_url = "http://10.0.10.178/log?dag_id={}&task_id={}&execution_date={}".format(dag_id, task_id, context['ts'].split("+")[0])
    ##Possível enviar no Teams quando a carga falhar
    # teams_notification = MSTeamsWebhookOperator(
    #     task_id="msteams_notify_failure", trigger_rule="all_done",
    #     message="A DAG `{}` falhou na task: `{}`!<br>Duracao: `{}` segs.<br>Dono:`{}`.".format(dag_id, task_id,duration_time,owner_name),
    #     button_text="Veja o log", button_url=logs_url,
    #     theme_color="FF0000", http_conn_id='webhook-erro-airflow')
		
    #teams_notification.execute(context)

DEFAULT_ARGS = {
    'owner': 'Walter',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 11, 15, 30, 00, tzinfo=local_tz),
    'email': ['walterferreirajesus@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'email_on_failure': True,
    'email_on_retry': False,
    'on_success_callback': on_success,
    'on_failure_callback': on_failure,
}

with DAG(    
    dag_id='load_adult',
    default_args=DEFAULT_ARGS,
    schedule_interval='*/ * * * *',
    start_date=datetime(2022, 9, 19),
    max_active_runs=1,
    tags=['python','load','csv','ClickSign'],
) as dag:    
    job1 = BashOperator(
        task_id='loadData',
        bash_command='/srv/samba/shared/kettles/python_etl/venv/bin/python /srv/samba/shared/kettles/python_etl/app/IngestaoAdult.py',
        params={})