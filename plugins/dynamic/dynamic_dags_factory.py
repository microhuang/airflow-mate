import os

from airflow import models, plugins_manager, settings, configuration

from airflow.models import Variable

from dynamic.dynamic_dags_variable import create_all, create_dag_from_nodes_edges_tasks, bpmn_file_to_dag_data


# from variable
'''
dags = Variable.get('dynamic-dags', [], True)
dags = create_all(dags)
#dags = []
for dag_id in dags:
    globals()[dag_id] = dags.get(dag_id)
'''

# from bpmn

dags_folder = configuration.get('core', 'DAGS_FOLDER')
bpmns = os.listdir(dags_folder)
for bpmn in bpmns:
    if bpmn.endswith('.bpmn'):
        file_path = os.path.join(dags_folder, bpmn)
        dag, dag_id = bpmn_file_to_dag_data(file_path)
        if dag:
            #globals()[dag_id] = dag
            pass
