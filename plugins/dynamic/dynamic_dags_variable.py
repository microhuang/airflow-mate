import os
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.configuration import conf
from airflow.operators.python import PythonOperator, ShortCircuitOperator, BranchPythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.operators.email import EmailOperator
#from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.utils.state import State
from airflow.models import Variable
from xml.dom.minidom import parse

from dynamic.gitop import GitPull

SFTPOperator.template_fields = ('local_filepath', 'remote_filepath', 'remote_host', 'ssh_conn_id', 'operation')


def slicing_task_process(slicing):
                    slicing_list = []
                    if slicing.get('slicing_type','auto')=='auto':
                        for i in range(1, slicing.get('slicing_auto',0)+1):
                            slicing_list.append(i)
                    elif slicing.get('slicing_type','auto')=='manual':
                        for i in slicing.get('slicing_manual','').split(','):
                            slicing_list.append(i)
                    else:
                        raise Exception("错误的分片类型："+slicing.get('slicing_type','auto')+"，分片类型为：auto or manual！！！")
                    if slicing_list:
                        _group = TaskGroup(task_id)
                        _[task_id] = _group
                        for i in slicing_list:
                            env = i
                            _task = BashOperator(
                                task_id=task_id+'-'+str(i),
                                bash_command=tasks.get(task_id,{}).get('params',{}).get('bash_command'),
                                email=email,
                                trigger_rule=tasks.get(task_id,{}).get('params',{}).get('trigger_rule', 'all_success'),
                                env=env,
                                dag=dag)
                            _group.add(_task)

def create_dag_from_nodes_edges_tasks(dag_info, nodes, edges, tasks):

    def skip_dag_pythonOperator(ds, **context):
        print('skip_dag:', context)
        print('task_log:', context.get('ti'))
        from datetime import datetime
        import pytz
        #if context['dag_run'] and context['dag_run'].external_trigger:
        #    print('Externally triggered DAG_Run: allowing execution to proceed.')
        #    return True
        skip = False
        if context.get('skip_dag_when_not_latest_worker', True):
            print('skip_dag_when_not_latest_worker')
            skip = False
            now = datetime.now()
            now = now.replace(tzinfo=pytz.timezone('UTC'))
            left_window = context['dag'].following_schedule(datetime.strptime(context['execution_date'], '%Y-%m-%d %H:%M:%S%z'))
            right_window = context['dag'].following_schedule(left_window)
            if not left_window < now <= right_window:
                skip = True
            #return not skip
        if skip and context.get('skip_dag_when_previous_running_worker', True):
            print('skip_dag_when_previous_running_worker')
            skip = False
            from airflow import settings
            from airflow.models import DagRun
            session = settings.Session()
            count = session.query(DagRun).filter(
                            DagRun.dag_id == context['dag'].dag_id,
                            DagRun.state.in_(['running']),
                    ).count()
            session.close()
            skip = count > 1
            #return not skip
        return not skip

    def call_pythonOperator(ds, **kwargs):
        print('kwargs', kwargs)
        #print('bpmn', kwargs.get('bpmn',''), 'flow', kwargs.get('flow',''))
        # workflow
        if kwargs.get('bpmn','') or kwargs.get('flow',''):
          from lxml import etree
          from SpiffWorkflow.bpmn.workflow import BpmnWorkflow
          from SpiffWorkflow.camunda.parser.CamundaParser import CamundaParser
          from SpiffWorkflow.camunda.specs.UserTask import EnumFormField, UserTask
          parser = CamundaParser()
          flow_id = ''
          # TODO: xxxx
          if kwargs.get('bpmn',''):
              bpmn_xml = kwargs.get('bpmn')
              sub_id = bpmn_xml.split('"')[1]
              print('sub_id', sub_id)
              bpmn_xml = '<?xml version="1.0" encoding="UTF-8"?>'+'\n'+'<bpmn2:definitions xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:bpmn2="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" id="sample-diagram" targetNamespace="http://bpmn.io/schema/bpmn" xsi:schemaLocation="http://www.omg.org/spec/BPMN/20100524/MODEL BPMN20.xsd">'+'\n'+'<bpmn2:process id="Process_1" isExecutable="false" type="Dag">'+'\n'+bpmn_xml+'\n'+'<bpmn2:startEvent id="Event_1o5n8fl"></bpmn2:startEvent>'+'\n'+'<bpmn2:endEvent id="Event_0z0hd6r"></bpmn2:endEvent>'+'\n'+'<bpmn2:sequenceFlow id="Flow_0rco9w1" sourceRef="Event_1o5n8fl" targetRef="'+sub_id+'"/>'+'\n'+'<bpmn2:sequenceFlow id="Flow_08h7s5a" sourceRef="'+sub_id+'" targetRef="Event_0z0hd6r"/>'+'\n'+'</bpmn2:process>'+'\n'
              bpmn_xml = bpmn_xml+'\n'+'</bpmn2:definitions>'
              print('xml: ', bpmn_xml)
              bpmn_xml = etree.fromstring(bpmn_xml.encode())
              #if if bpmn_xml.getchildren()[0].get('type')=='Dag':
              #    raise Exception('XML format error')
              flow_id = bpmn_xml.getchildren()[0].get('id')
              if not flow_id:
                  flow_id = bpmn_xml.getchildren()[0].get('name')
              #parser.add_bpmn_file(bpmn_xml)
              parser.add_bpmn_xml(bpmn_xml)
          else:
              dags_folder = conf.get('core', 'DAGS_FOLDER')
              with open(os.path.join(dags_folder, kwargs.get('flow','')+'.bpmn'), 'r') as f:
                  bpmn_xml = etree.fromstring(f.read().encode())
                  if bpmn_xml.getchildren()[0].get('type')=='Dag':
                      raise Exception('XML format error')
                  flow_id = bpmn_xml.getchildren()[0].get('id')
                  if not flow_id:
                      flow_id = bpmn_xml.getchildren()[0].get('name')
                  parser.add_bpmn_xml(bpmn_xml)
                  #parser.add_bpmn_file(etree.XML(bytes(bytearray(f.read(),encoding='utf-8'))))
          #spec = parser.get_spec(kwargs.get('task_id'))
          spec = parser.get_spec(flow_id)
          workflow = BpmnWorkflow(spec)
          workflow.do_engine_steps()
          ready_tasks = workflow.get_ready_user_tasks()
          while len(ready_tasks) > 0:
            for task in ready_tasks:
                if isinstance(task.task_spec, UserTask):
                    pass
                else:
                    pass
                workflow.complete_task_from_id(task.id)
            workflow.do_engine_steps()
            ready_tasks = workflow.get_ready_user_tasks()
          # return ok
        # pthyon
        elif kwargs.get('main_module') or kwargs.get('main_function'):
            return slicing_pythonOperator(ds, **kwargs)
        # other
        else:
            raise Exception('Missing required parameters: main_module or main_function')

    def slicing_pythonOperator(ds, **kwargs):
        main_module = kwargs.get('main_module', None)
        print('slicing_pythonOperator:', main_module, kwargs)
        if main_module == 'dynamic_dags_task':
            # 随机模拟修改动态分片
            key = 'dynamic-dag-'+dag_info.get('dag_id')
            data = Variable.get(key, None, True)
            if data:
                slicing_task = kwargs.get('kwargs',{}).get('slicing_task')
                if slicing_task and kwargs.get('kwargs',{}).get('slicing'):
                    flag = False
                    import random
                    nums = random.randrange(10)
                    data['tasks'][slicing_task]['params']['slicing']['slicing_auto'] = nums
                    if kwargs.get('kwargs',{}).get('slicing',{}).get('slicing_auto'):
                        value = kwargs.get('kwargs',{}).get('slicing',{}).get('slicing_auto')
                        if not value == data['tasks'][slicing_task]['params']['slicing']['slicing_auto']:
                            data['tasks'][slicing_task]['params']['slicing']['slicing_auto'] = value
                            flag = True
                    if kwargs.get('kwargs',{}).get('slicing',{}).get('slicing_manual'):
                        value = kwargs.get('kwargs',{}).get('slicing',{}).get('slicing_manual')
                        if not value == data['tasks'][slicing_task]['params']['slicing']['slicing_manual']:
                            data['tasks'][slicing_task]['params']['slicing']['slicing_manual'] = value
                            flag = True
                    if kwargs.get('kwargs',{}).get('slicing',{}).get('slicing_type'):
                        value = kwargs.get('kwargs',{}).get('slicing',{}).get('slicing_type')
                        if not value == data['tasks'][slicing_task]['params']['slicing']['slicing_type']:
                            data['tasks'][slicing_task]['params']['slicing']['slicing_type'] = value
                            flag = True
                    if kwargs.get('kwargs',{}).get('slicing',{}).get('enable_slicing'):
                        value = kwargs.get('kwargs',{}).get('slicing',{}).get('enable_slicing')
                        if not value == data['tasks'][slicing_task]['params']['slicing']['enable_slicing']:
                            data['tasks'][slicing_task]['params']['slicing']['enable_slicing'] = value
                            flag = True

                    print('改变动态分片：'+str(nums))
                    #dag_run = dag.get_dagrun(run_id=kwargs.get('run_id'))
                    #print(dag_run.execution_date)
                    old_time = kwargs.get('execution_date', None)
                    if flag and old_time:
                        Variable.set(key, data, True)
                        import time
                        time.sleep(conf.getint('scheduler', 'min_file_process_interval')) # TODO: 等待刷新, scheduler从序列化获取dag，需要更合理的方式确保dag已经更新
                        dag_run = dag.get_dagrun(run_id=kwargs.get('run_id'))
                        new_time = dag_run.execution_date
                        while new_time==old_time:
                            time.sleep(conf.getint('scheduler', 'min_file_process_interval'))
                            dag_run = dag.get_dagrun(run_id=kwargs.get('run_id'))
                            new_time = dag_run.execution_date
        elif isinstance(main_module,str) and main_module:
            from importlib import import_module
            module = import_module(kwargs.get('main_module'))
            if isinstance(kwargs.get('main_function'),str) and kwargs.get('main_function'):
                func = getattr(module, kwargs.get('main_function'))
                main_args = kwargs.get('main_args')
                ret = func(main_args)
        elif isinstance(kwargs.get('main_function'),str) and kwargs.get('main_function'):
            import builtins
            func = getattr(builtins, kwargs.get('main_function'))
            main_args = kwargs.get('main_args')
            ret = func(main_args)

    '''
    default_args = {'owner': 'airflow',
                    'start_date': datetime(2018, 1, 1)
                    }'''
    now_time=datetime.now()
    start_date=now_time+timedelta(hours=-1)
    if dag_info.get('start_date'):
        start_date=dag_info.get('start_date')
        # 两种格式
        try:
            start_date=datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        except ValueError as ex:
            start_date=datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S%z')
    end_date=None
    if dag_info.get('end_date'):
        try:
            end_date=dag_info.get('end_date')
            end_date=datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        except ValueError as ex:
            end_date=datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S%z')
    default_args = {
    'owner': dag_info.get('owner','airflow'),
    #'owners': 'p1,p2',
    'start_date': start_date,
    'end_date': end_date,
    #'retries': %(retries)s,
    #'retry_delay': timedelta(minutes=%(retry_delay_minutes)s),
    'depends_on_past': False,
    #'email': ['airflow2@xxx.com'],
    'email': dag_info.get('email'),
    #'email_on_failure': True,
    'email_on_failure': dag_info.get('email_on_failure'),
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
    }

    _env = {
            "enable_slicing":True # subprocess 不能接收非字符串值
            ,"slicing_type":'auto' # or manual
            ,"slicing_auto":"2" # 1、 2
            ,"slicing_manual":"1-2, 4"
            #,"name":"dynamic-dags"
            }
    #_env = [{'name': 'AIRFLOW_IS_K8S_EXECUTOR_POD', 'value': 'True'}]
    if _env.get('enable_slicing', False)==True:
        _env['enable_slicing'] = True
    else:
        _env['enable_slicing'] = False
    for k in _env:
        if not isinstance(_env.get(k),str):
            _env[k] = str(_env.get(k))

    _ = {}
    groups = {}
    dagrun_timeout=dag_info.get('dagrun_timeout', None)
    if isinstance(dagrun_timeout, str):
        if dagrun_timeout:
            dagrun_timeout = timedelta(seconds=int(dagrun_timeout))
        else:
            dagrun_timeout = None
    dag_info['dagrun_timeout'] = dagrun_timeout
    dag = DAG(dag_info.get('dag_id'),
            description=dag_info.get('description',''),
            start_date=start_date,
            end_date=end_date,
            schedule_interval=dag_info.get('schedule_interval'),
            dagrun_timeout=dagrun_timeout,
            catchup=dag_info.get('catchup', False),
            tags=dag_info.get('tags', None),
            default_args=default_args)
    with dag:
        for task_id in tasks:
            '''
            _env = {
                "enable_slicing":True # subprocess 不能接收非字符串值
                ,"slicing_type":'auto' # or manual
                ,"slicing_auto":"2" # 1、 2
                ,"slicing_manual":"1-2, 4"
                #,"name":"dynamic-dags"
            }
            '''
            _env = tasks.get(task_id).get('params',{}).get('slicing',{})
            if _env.get('enable_slicing', False)==True:
                _env['enable_slicing'] = True
            else:
                _env['enable_slicing'] = False
            for k in _env:
                if not isinstance(_env.get(k),str):
                    _env[k] = str(_env.get(k))
            group = None
            '''
            ids = task_id.split('.')
            ids.pop()
            gid = ''
            for i in ids:
                p_group = None
                if groups.get(gid):
                    p_group = groups.get(gid)
                gid = gid + i
                if not groups.get(gid):
                  try:
                    group = TaskGroup(i, tooltip=gid)
                    groups[gid] = group
                    _[gid] = group
                  except Exception as err:
                    print('err: dag_id,', dag.dag_id)
                    raise err
                else:
                    group = groups[gid]
                if p_group:
                    p_group.add(group)
            task = None
            if tasks.get(task_id).get('task_group'):
                group = groups.get(tasks.get(task_id).get('task_group'), None)
                if not group:
                    group = TaskGroup(tasks.get(task_id).get('task_group'), tooltip=tasks.get(task_id).get('task_group'), dag=dag)
                    groups[tasks.get(task_id).get('task_group')] = group
            '''
            execution_timeout = tasks.get(task_id).get('params',{}).get('execution_timeout') # TODO:xxx
            #execution_timeout = timedelta(hours=1)
            if isinstance(execution_timeout, str):
                if execution_timeout:
                    execution_timeout = timedelta(seconds=int(execution_timeout))
                else:
                    execution_timeout = None
            email = default_args.get('email')
            if tasks.get(task_id).get('params',{}).get('email'):
                email = tasks.get(task_id).get('params',{}).get('email')
            task_queue = tasks.get(task_id).get('params',{}).get('queue')
            dag_queue = dag_info.get('queue')
            if dag_queue:
                task_queue = dag_queue
            if not task_queue:
                task_queue = conf.get('operators', 'default_queue')
            if tasks.get(task_id).get('task_type')=='SqlSensor':
                task = SqlSensor(
                        task_id=task_id,
                        conn_id='local_mysql',
                        queue=task_queue,
                        #sql="SELECT count(%s) FROM INFORMATION_SCHEMA.TABLES",
                        #parameters=["table_name"],
                        sql="select dag_id, last_updated from serialized_dag where dag_id=%s and last_updated!=%s;",
                        parameters=[dag.dag_id, int(123)],
                        trigger_rule=tasks.get(task_id,{}).get('params',{}).get('trigger_rule', 'all_success'),
                        dag=dag,
                        task_group=group
                        )
            elif tasks.get(task_id).get('task_type')=='ShortCircuitOperator':
                task = ShortCircuitOperator(
                        task_id=task_id,
                        queue=task_queue,
                        #provide_context=True,
                        op_kwargs={"skip_dag_when_not_latest_worker":tasks.get(task_id).get("params",{}).get('skip_dag_when_not_latest_worker', False), "skip_dag_when_previous_running_worker":tasks.get(task_id).get("params",{}).get('skip_dag_when_previous_running_worker', False), "execution_date":'{{ (dag_run.execution_date) }}'},
                        python_callable=skip_dag_pythonOperator,
                        trigger_rule=tasks.get(task_id,{}).get('params',{}).get('trigger_rule', 'all_success'),
                        dag=dag,
                        task_group=group
                        )
            elif tasks.get(task_id).get('task_type')=='PythonOperator':
                if not (tasks.get(task_id).get('params',{}).get('slicing',{}).get('enable_slicing','False')=='True'):
                    # run main module
                    task = PythonOperator(
                        task_id=task_id,
                        queue=task_queue,
                        #provide_context=True,
                        op_kwargs={"main_module":tasks.get(task_id).get("params",{}).get('main_module'), "main_function":tasks.get(task_id).get("params",{}).get('main_function'), "main_args":tasks.get(task_id).get("params",{}).get('main_args'), "kwargs":tasks.get(task_id).get("params",{}).get('kwargs'), "execution_date":'{{ (dag_run.execution_date) }}', "slicing_task":tasks.get(task_id).get("params",{}).get('slicing_task'),"bpmn":tasks.get(task_id).get('params',{}).get('bpmn'),"flow":tasks.get(task_id).get('params',{}).get('flow'),'task_id':task_id},
                        # TODO: xxxx
                        python_callable=call_pythonOperator,
                        execution_timeout=execution_timeout,
                        email=email,
                        trigger_rule=tasks.get(task_id,{}).get('params',{}).get('trigger_rule', 'all_success'),
                        dag=dag,
                        task_group=group
                        )
                else:
                    # slicing
                    #slicing_task_process(tasks.get(task_id).get('params',{}).get('slicing',{}))
                    slicing_list = []
                    if tasks.get(task_id).get('params',{}).get('slicing',{}).get('slicing_type','auto')=='auto':
                        for i in range(1, int(tasks.get(task_id).get('params',{}).get('slicing',{}).get('slicing_auto',0))+1):
                            slicing_list.append(i)
                    elif tasks.get(task_id).get('params',{}).get('slicing',{}).get('slicing_type','auto')=='manual':
                        for i in tasks.get(task_id).get('params',{}).get('slicing',{}).get('slicing_manual','').split(','):
                            slicing_list.append(i)
                    else:
                        raise Exception("错误的分片类型："+tasks.get(task_id).get('params',{}).get('slicing',{}).get('slicing_type','auto')+"，分片类型为：auto or manual！！！")
                    if slicing_list:
                        _group = TaskGroup(task_id)
                        #_[task_id] = _group
                        task = _group
                        for i in slicing_list:
                            env = _env.copy()
                            env['slicing_code'] = str(i)
                            _task = PythonOperator(
                                task_id=task_id+'-'+str(i),
                                queue=task_queue,
                                #provide_context=True,
                                op_kwargs={"main_module":tasks.get(task_id).get('params',{}).get('main_module'), "execution_date":'{{ (dag_run.execution_date) }}'},
                                python_callable=slicing_pythonOperator,
                                execution_timeout=execution_timeout,
                                email=email,
                                trigger_rule=tasks.get(task_id,{}).get('params',{}).get('trigger_rule', 'all_success'),
                                dag=dag,
                                task_group=group
                                )
                            _group.add(_task)
            elif tasks.get(task_id).get('task_type')=='SSHOperator':
                if not (tasks.get(task_id).get('params',{}).get('slicing',{}).get('enable_slicing','False')=='True'):
                #if tasks.get(task_id).get('params',{}).get('slicing',{}).get('enable_slicing',False)==False:
                    # run ssh command
                    task = SSHOperator(
                        task_id=task_id,
                        queue=task_queue,
                        ssh_conn_id=tasks.get(task_id,{}).get('params',{}).get('ssh_conn_id','ssh_default'), # or ssh_hook=
                        command=tasks.get(task_id,{}).get('params',{}).get('command'),
                        execution_timeout=execution_timeout,
                        email=email,
                        trigger_rule=tasks.get(task_id,{}).get('params',{}).get('trigger_rule', 'all_success'),
                        dag=dag,
                        task_group=group
                        )
                else:
                    # slicing
                    #slicing_task_process(tasks.get(task_id).get('params',{}).get('slicing',{}))
                    slicing_list = []
                    if tasks.get(task_id).get('params',{}).get('slicing',{}).get('slicing_type','auto')=='auto':
                        for i in range(1, int(tasks.get(task_id).get('params',{}).get('slicing',{}).get('slicing_auto',0))+1):
                            slicing_list.append(i)
                    elif tasks.get(task_id).get('params',{}).get('slicing',{}).get('slicing_type','auto')=='manual':
                        for i in tasks.get(task_id).get('params',{}).get('slicing',{}).get('slicing_manual','').split(','):
                            slicing_list.append(i)
                    else:
                        raise Exception(task_id+"错误的分片类型："+tasks.get(task_id).get('params',{}).get('slicing',{}).get('slicing_type','auto')+"，分片类型为：auto or manual！！！")
                    if slicing_list:
                        _group = TaskGroup(task_id)
                        #_[task_id] = _group
                        task = _group
                        for i in slicing_list:
                            env = _env.copy()
                            env['slicing_code'] = str(i)
                            _task = SSHOperator(
                                task_id=task_id+'-'+str(i),
                                queue=task_queue,
                                ssh_conn_id=tasks.get(task_id,{}).get('params',{}).get('ssh_conn_id','ssh_default'), # or ssh_hook=
                                command=tasks.get(task_id,{}).get('params',{}).get('command'),
                                execution_timeout=execution_timeout,
                                email=email,
                                trigger_rule=tasks.get(task_id,{}).get('params',{}).get('trigger_rule', 'all_success'),
                                dag=dag,
                                task_group=group
                                )
                            _group.add(_task)
            elif tasks.get(task_id).get('task_type')=='BashOperator':
                import textwrap
                templated_command = textwrap.dedent('''
                        echo "start"
                        ds="{{ds}}"
                        {{params.bash_command}}
                        echo "end"
                        '''
                        #.format(bash_command=tasks.get(task_id,{}).get('params',{}).get('bash_command'))
                        )
                templated_command.format(ds='{{ds}}')
                env = {'execution_date':'{{execution_date}}', 'ds':'{{ds}}', 'next_ds':'{{next_ds}}', 'prev_ds':'{{prev_ds}}'}
                env.update(dict(os.environ))
                if not tasks.get(task_id).get('params',{}).get('slicing',{}).get('enable_slicing','False')=='True':
                    # run bash_command
                    task = BashOperator(
                        task_id=task_id,
                        queue=task_queue,
                        bash_command=templated_command,
                        #bash_command=tasks.get(task_id,{}).get('params',{}).get('bash_command'),
                        params={'bash_command':tasks.get(task_id,{}).get('params',{}).get('bash_command')},
                        env=env,
                        execution_timeout=execution_timeout,
                        email=email,
                        trigger_rule=tasks.get(task_id,{}).get('params',{}).get('trigger_rule', 'all_success'),
                        dag=dag,
                        task_group=group
                        )
                else:
                    # slicing
                    #slicing_task_process(tasks.get(task_id).get('params',{}).get('slicing',{}))
                    #print('eeeeeeeeeeeeeeeeeeeeeee', tasks.get(task_id).get('params',{}).get('slicing',{}).get('enable_slicing',False), task_id)
                    slicing_list = []
                    if tasks.get(task_id).get('params',{}).get('slicing',{}).get('slicing_type','auto')=='auto':
                        for i in range(1, int(tasks.get(task_id).get('params',{}).get('slicing',{}).get('slicing_auto',0))+1):
                            slicing_list.append(i)
                    elif tasks.get(task_id).get('params',{}).get('slicing',{}).get('slicing_type','auto')=='manual':
                        for i in tasks.get(task_id).get('params',{}).get('slicing',{}).get('slicing_manual','').split(','):
                            slicing_list.append(i)
                    else:
                        raise Exception("错误的分片类型："+tasks.get(task_id).get('params',{}).get('slicing',{}).get('slicing_type','auto')+"，分片类型为：auto or manual！！！")
                    if slicing_list:
                        _group = TaskGroup(task_id, tooltip="这是一个动态分片节点！！！")
                        _group.task_type = 'BashOperator'
                        _group.node_type = 'task' # 'group' 'task' 'child'
                        #_[task_id] = _group
                        task = _group
                        for i in slicing_list:
                            env = _env.copy()
                            env['slicing_code'] = str(i)
                            #print('cccccccccccccccc', str(i))
                            _task = BashOperator(
                                task_id=task_id+'-'+str(i),
                                queue=task_queue,
                                bash_command=tasks.get(task_id,{}).get('params',{}).get('bash_command'),
                                env=env,
                                execution_timeout=execution_timeout,
                                email=email,
                                trigger_rule=tasks.get(task_id,{}).get('params',{}).get('trigger_rule', 'all_success'),
                                dag=dag,
                                #task_group=group
                                )
                            _task.node_type = 'child'
                            _group.add(_task)
            elif tasks.get(task_id).get('task_type')=='ExternalTaskMarker':
                task = ExternalTaskMarker(
                        task_id=task_id,
                        queue=task_queue,
                        external_dag_id=tasks.get(task_id,{}).get('params',{}).get('external_dag_id'),
                        external_task_id=tasks.get(task_id,{}).get('params',{}).get('external_task_id'),
                        dag=dag
                        )
            elif tasks.get(task_id).get('task_type')=='ExternalTaskSensor':
                task = ExternalTaskSensor(
                        task_id=task_id,
                        queue=task_queue,
                        external_dag_id=tasks.get(task_id,{}).get('params',{}).get('external_dag_id'),
                        external_task_id=tasks.get(task_id,{}).get('params',{}).get('external_task_id'),
                        #execution_delta=timedelta(hours=8),
                        #mode='reschedule', # poke
                        execution_timeout=execution_timeout,
                        check_existence=False,
                        dag=dag
                        )
            elif tasks.get(task_id).get('task_type')=='SubDagOperator':
                task = SubDagOperator(
                        task_id=task_id,
                        queue=task_queue,
                        #subdag=
                        dag=dag
                        )
            elif tasks.get(task_id).get('task_type')=='SFTPOperator':
                task = SFTPOperator(
                        task_id=task_id,
                        queue=task_queue,
                        ssh_conn_id=tasks.get(task_id,{}).get('params',{}).get('ssh_conn_id','ssh_default'),
                        local_filepath=tasks.get(task_id,{}).get('params',{}).get('local_filepath',''),
                        remote_filepath=tasks.get(task_id,{}).get('params',{}).get('remote_filepath',''),
                        operation=tasks.get(task_id,{}).get('params',{}).get('operation',''), # From Local to Remote
                        create_intermediate_dirs=True,
                        dag=dag,
                        task_group=group
                        )
            elif tasks.get(task_id).get('task_type')=='DummyOperator':
                task = DummyOperator(
                        task_id=task_id,
                        queue=task_queue,
                        dag=dag,task_group=group
                        )
            else:
                pass

            # external
            if tasks.get(task_id,{}).get('params',{}).get('external_dag_id') and tasks.get(task_id,{}).get('params',{}).get('external_task_ids'):
                e_task = ExternalTaskSensor(
                        task_id=tasks.get(task_id,{}).get('params',{}).get('external_dag_id')+"-"+tasks.get(task_id,{}).get('params',{}).get('external_task_ids'),
                        external_dag_id=tasks.get(task_id,{}).get('params',{}).get('external_dag_id'),
                        external_task_id=tasks.get(task_id,{}).get('params',{}).get('external_task_ids'),
                        timeout=int(tasks.get(task_id,{}).get('params',{}).get('timeout', 60)),
                        queue=task_queue,
                        dag=dag
                        )
                e_task >> task
            '''
            # before
            if tasks.get(task_id,{}).get('params',{}).get('before_dag_id') and tasks.get(task_id,{}).get('params',{}).get('before_task_ids'):
                b_task = ExternalTaskSensor(
                        task_id=tasks.get(task_id,{}).get('params',{}).get('before_dag_id')+"-"+tasks.get(task_id,{}).get('params',{}).get('before_task_ids'),
                        external_dag_id=tasks.get(task_id,{}).get('params',{}).get('before_dag_id'),
                        external_task_id=tasks.get(task_id,{}).get('params',{}).get('before_task_ids'),
                        timeout=int(tasks.get(task_id,{}).get('params',{}).get('timeout', 60)),
                        queue=task_queue,
                        dag=dag
                        )
                b_task >> task
            # after
            if tasks.get(task_id,{}).get('params',{}).get('after_dag_id') and tasks.get(task_id,{}).get('params',{}).get('after_task_ids'):
                a_task = ExternalTaskSensor(
                        task_id=tasks.get(task_id,{}).get('params',{}).get('after_dag_id')+"-"+tasks.get(task_id,{}).get('params',{}).get('after_task_ids'),
                        external_dag_id=tasks.get(task_id,{}).get('params',{}).get('external_dag_id'),
                        external_task_id=tasks.get(task_id,{}).get('params',{}).get('external_task_ids'),
                        timeout=60,
                        queue=task_queue,
                        dag=dag
                        )
                task >> a_task
            '''

            if task:
                #if group:
                #    group.add(task)
                _[task_id] = task
                if tasks.get(task_id).get('task_group'):
                    group = groups.get(tasks.get(task_id).get('task_group'), None)
                    if not group:
                        group = TaskGroup(tasks.get(task_id).get('task_group'), tooltip=tasks.get(task_id).get('task_group'), dag=dag)
                        groups[tasks.get(task_id).get('task_group')] = group
        for edge in edges:
            _[edge.get('source_id')] >> _[edge.get('target_id')]

    return dag


def bpmn_file_to_dag_data(bpmn_path):
    try:
            ids = {}
            info = {}
            tasks = {}
            edges = []
            nodes = {}
            bpmn_ver = "bpmn2:"
            domTree = parse(bpmn_path)
            rootNode = domTree.documentElement
            processNode = rootNode.getElementsByTagName(bpmn_ver+'process')
            if not processNode:
                processNode = rootNode.getElementsByTagName('process')
                if processNode:
                    bpmn_ver = ""
                    processNode = processNode[0]
            else:
                processNode = processNode[0]
            if processNode.getAttribute('type')=='Flow' or processNode.getAttribute('airflow_type')=='Flow':
                return None, None
            dag_id =  processNode.getAttribute('id')
            dag_name = processNode.getAttribute('name')
            if dag_name:
                dag_id = dag_name
            info['dag_id'] = dag_id
            dag_queue = processNode.getAttribute('queue')
            info['queue'] = dag_queue
            owner = processNode.getAttribute('owner')
            info['owner'] = owner
            description = processNode.getAttribute('description')
            documentationRef = processNode.getAttribute('documentationRef')
            if documentationRef and not description:
                try:
                    description = rootNode.getElementsByTagName(bpmn_ver+'documentation')[0].childNodes[0].data
                except Exception as ex:
                    print(dag_id, ', description, ', ex)
            info['description'] = description
            start_date = processNode.getAttribute('start')
            info['start_date'] = start_date
            end_date = processNode.getAttribute('end')
            info['end_date'] = end_date
            start_date = processNode.getAttribute('start_date')
            if start_date:
                info['start_date'] = start_date
            end_date = processNode.getAttribute('end_date')
            if end_date:
                info['end_date'] = end_date
            schedule_interval = processNode.getAttribute('schedule')
            info['schedule_interval'] = schedule_interval
            dagrun_timeout = processNode.getAttribute('dagrun_timeout')
            info['dagrun_timeout'] = dagrun_timeout
            timeout = processNode.getAttribute('timeout')
            if timeout:
                info['dagrun_timeout'] = timeout
            catchup = processNode.getAttribute('catchup')
            if str(catchup).upper()=='TRUE':
                info['catchup'] = True
            else:
                info['catchup'] = False
            tags = processNode.getAttribute('tags')
            info['tags'] = [tags]
            email = processNode.getAttribute('email')
            info['email'] = email
            # group
            shapes = {}
            groups = {}
            groupsNode = processNode.getElementsByTagName(bpmn_ver+'group')
            for groupNode in groupsNode:
                group_id = groupNode.getAttribute('id')
                group_name = groupNode.getAttribute('name')
                if not group_name:
                    group_name = group_id
                groups[group_id] = {'name':group_name}
            # x,y,w,h
            groupRoot = rootNode.getElementsByTagName('bpmndi:BPMNDiagram')[0]
            groupRoot = groupRoot.getElementsByTagName('bpmndi:BPMNPlane')[0]
            groupsNode = groupRoot.getElementsByTagName('bpmndi:BPMNShape')
            for groupNode in groupsNode:
                group_id = groupNode.getAttribute('bpmnElement')
                node = groupNode.getElementsByTagName('dc:Bounds')[0]
                if groups.get(group_id,None):
                    #groups[group_id] = {}
                    groups[group_id]['x'] = node.getAttribute('x')
                    groups[group_id]['y'] = node.getAttribute('y')
                    groups[group_id]['width'] = node.getAttribute('width')
                    groups[group_id]['height'] = node.getAttribute('height')
                else:
                    shapes[group_id] = {}
                    shapes[group_id]['x'] = node.getAttribute('x')
                    shapes[group_id]['y'] = node.getAttribute('y')
                    shapes[group_id]['width'] = node.getAttribute('width')
                    shapes[group_id]['height'] = node.getAttribute('height')
            # task
            tasksNode = processNode.getElementsByTagName(bpmn_ver+'task')
            for taskNode in tasksNode:
                task_id = taskNode.getAttribute('id').strip()
                # task group
                task_group = None
                if task_id in shapes:
                    for group in groups:
                        #print('ggggggggg', task_id, int(shapes.get(task_id,{}).get('x',0))>int(groups.get(group).get('width',0)) and int(shapes.get(task_id,{}).get('x',0))+int(shapes.get(task_id,{}).get('width',0))<int(groups.get(group).get('x',0))+int(groups.get(group).get('width',0)))
                        if int(shapes.get(task_id,{}).get('x',0))>int(groups.get(group).get('width',0)) and int(shapes.get(task_id,{}).get('x',0))+int(shapes.get(task_id,{}).get('width',0))<int(groups.get(group).get('x',0))+int(groups.get(group).get('width',0)):
                            task_group = groups.get(group).get('name')
                            break
                task_name = taskNode.getAttribute('name').strip()
                ids[task_id] = task_id
                if task_name:
                    ids[task_id] = task_name
                    task_id = task_name
                tasks[task_id] = {}
                params = {}
                task_type = taskNode.getAttribute('taskType')
                if task_type=='FlowOperator':
                    task_type='PythonOperator'
                    flow=taskNode.getAttribute('flow')
                    params['flow'] = flow
                tasks[task_id] = {'dag_id':dag_id, 'task_type':task_type, 'task_group':task_group, 'params':params}
                # base
                email = taskNode.getAttribute('email')
                tasks[task_id]['params']['email'] = email
                timeout = taskNode.getAttribute('timeout')
                tasks[task_id]['params']['timeout'] = timeout
                #execution_timeout = taskNode.getAttribute('execution_timeout')
                #tasks[task_id]['params']['execution_timeout'] = execution_timeout
                tasks[task_id]['params']['execution_timeout'] = timeout
                trigger_rule = taskNode.getAttribute('trigger_rule')
                if trigger_rule:
                    tasks[task_id]['params']['trigger_rule'] = trigger_rule
                # slicing
                tasks[task_id]['params']['slicing'] = {}
                enable_slicing = taskNode.getAttribute('enable_slicing')
                if str(enable_slicing).upper()=='TRUE':
                    enable_slicing = True
                else:
                    enable_slicing = False
                tasks[task_id]['params']['slicing']['enable_slicing'] = enable_slicing
                slicing_type = taskNode.getAttribute('slicing_type')
                tasks[task_id]['params']['slicing']['slicing_type'] = slicing_type
                slicing_auto = taskNode.getAttribute('slicing_auto')
                tasks[task_id]['params']['slicing']['slicing_auto'] = slicing_auto
                slicing_manual = taskNode.getAttribute('slicing_manual')
                tasks[task_id]['params']['slicing']['slicing_manual'] = slicing_manual
                task_queue = taskNode.getAttribute('queue')
                tasks[task_id]['params']['queue'] = task_queue
                # external
                external_dag_id = taskNode.getAttribute('external_dag_id')
                external_task_ids = taskNode.getAttribute('external_task_ids')
                if external_dag_id and external_task_ids:
                    tasks[task_id]['params']['external_dag_id'] = external_dag_id
                    tasks[task_id]['params']['external_task_ids'] = external_task_ids
                    execution_delta = taskNode.getAttribute('execution_delta')
                    if not execution_delta:
                        execution_delta = None
                    tasks[task_id]['params']['execution_delta'] = execution_delta
                # before
                enable_pre_action = taskNode.getAttribute('enable_pre_action')
                if enable_pre_action:
                    tasks[task_id]['params']['enable_pre_action'] = enable_pre_action
                # after
                enable_post_action = taskNode.getAttribute('enable_post_action')
                if enable_post_action:
                    tasks[task_id]['params']['enable_post_action'] = enable_post_action
                # operator
                if task_type=='SSHOperator':
                    ssh_conn_id = taskNode.getAttribute('ssh_conn_id')
                    tasks[task_id]['params']['ssh_conn_id'] = ssh_conn_id
                    #timeout = taskNode.getAttribute('timeout')
                    #tasks[task_id]['params']['timeout'] = timeout
                    command = taskNode.getAttribute('command')
                    tasks[task_id]['params']['command'] = command
                elif task_type=='ExternalTaskSensor' or task_type=='ExternalTaskMarker':
                    external_dag_id = taskNode.getAttribute('external_dag_id')
                    tasks[task_id]['params']['external_dag_id'] = external_dag_id
                    external_task_id = taskNode.getAttribute('external_task_id')
                    tasks[task_id]['params']['external_task_id'] = external_task_id
                    execution_delta = taskNode.getAttribute('execution_delta')
                    tasks[task_id]['params']['execution_delta'] = execution_delta
                elif task_type=='SubDagOperator':
                    subdag_id = taskNode.getAttribute('subdag_id')
                    tasks[task_id]['params']['subdag_id'] = subdag_id
                elif task_type=='SFTPOperator':
                    ssh_conn_id = taskNode.getAttribute('ssh_conn_id')
                    tasks[task_id]['params']['ssh_conn_id'] = ssh_conn_id
                    local_filepath = taskNode.getAttribute('local')
                    tasks[task_id]['params']['local_filepath'] = local_filepath
                    remote_filepath = taskNode.getAttribute('remote')
                    tasks[task_id]['params']['remote_filepath'] = remote_filepath
                    operation = taskNode.getAttribute('operation')
                    tasks[task_id]['params']['operation'] = operation
                elif task_type=='BashOperator':
                    command = taskNode.getAttribute('command')
                    tasks[task_id]['params']['bash_command'] = command
                elif task_type=='PythonOperator':
                    main_function = taskNode.getAttribute('main_function')
                    main_module = taskNode.getAttribute('main_module')
                    main_args = taskNode.getAttribute('main_args')
                    tasks[task_id]['params']['main_function'] = main_function
                    tasks[task_id]['params']['main_module'] = main_module
                    tasks[task_id]['params']['main_args'] = main_args
                elif task_type=='ShortCircuitOperator':
                    not_latest = taskNode.getAttribute('skip_dag_when_not_latest_worker')
                    if not_latest and str(not_latest).upper()=='TRUE':
                        tasks[task_id]['params']['skip_dag_when_not_latest_worker'] = True
                    previous_running = taskNode.getAttribute('skip_dag_when_previous_running_worker')
                    if previous_running and str(previous_running).upper()=='TRUE':
                        tasks[task_id]['params']['skip_dag_when_previous_running_worker'] = True
                    #print('eeeeeee', tasks)
            # sub process <=> node => python operator
            tasksNode = processNode.getElementsByTagName(bpmn_ver+'subProcess')
            for taskNode in tasksNode:
                task_id = taskNode.getAttribute('id')
                task_name = taskNode.getAttribute('name')
                ids[task_id] = task_id
                if task_name:
                    task_group = None
                    for group in groups:
                        if int(shapes.get(task_id,{}).get('x',0))>int(groups.get(group).get('width',0)) and int(shapes.get(task_id,{}).get('x',0))+int(shapes.get(task_id,{}).get('width',0))<int(groups.get(group).get('x',0))+int(groups.get(group).get('width',0)):
                            task_group = groups.get(group).get('name')
                    ids[task_id] = task_name
                    task_id = task_name
                    task_type = 'PythonOperator'
                    tasks[task_id] = {'dag_id':dag_id, 'task_type':task_type}
                    tasks[task_id]['params'] = {}
                    tasks[task_id]['params']['bpmn'] = taskNode.toxml()
                    tasks[task_id]['task_group'] = task_group
            # edges
            edgesNode = processNode.getElementsByTagName(bpmn_ver+'sequenceFlow')
            for edgeNode in edgesNode:
                if edgeNode.parentNode==processNode and (edgeNode.getAttribute('sourceRef') in ids and edgeNode.getAttribute('targetRef') in ids):
                    edges.append({'source_id':ids[edgeNode.getAttribute('sourceRef')], 'target_id':ids[edgeNode.getAttribute('targetRef')]})
                #print('edge', edgeNode.getAttribute('sourceRef'), edgeNode.getAttribute('targetRef'))
            #print('iiiiiiiii', info)
            dag = create_dag_from_nodes_edges_tasks(info, nodes, edges, tasks)
            print('created:', dag)
            #globals()[dag_id] = dag
            return dag, dag_id
    except Exception as err:
            raise(err)


    pass


def test(dags, data):
    for dag_id in dags:
        dag_data = data
        if dag_data:
            dag_data['info']['dag_id'] = dag_id
            dag = create_dag_from_nodes_edges_tasks(dag_data.get('info'), dag_data.get('nodes'), dag_data.get('edges'), dag_data.get('tasks'))
            print('test:', dag)
    return 0


BASE_NAME = 'dynamic-dag-'


def create_all(dags=[]):

    #Variable.set('dynamic-dags', ['a1','a2'], True)
    #Variable.delete('dynamic-dag-a1')
    #Variable.set('dynamic-dag-a1', {"nodes":{"children": [{"id": "t1", "value": {"label": "t1", "labelStyle": "fill:#000;", "rx": 5, "ry": 5, "style": "fill:#f0ede4;"}}], "id": None, "tooltip": "", "value": {"clusterLabelPos": "top", "label": None, "labelStyle": "fill:#000;", "rx": 5, "ry": 5, "style": "fill:CornflowerBlue"}},"tasks":{"t1": {"dag_id": "a1", "extra_links": [], "task_type": "BashOperator", "bash_command": "echo 1"},},"edges":[]}, True)

    if not dags:
        dags = Variable.get('dynamic-dags', [], True)
    dag_dict = {}
    for dag_id in dags:
        dag_data = Variable.get(BASE_NAME+dag_id, None, True)
        if dag_data:
            dag_data['info']['dag_id'] = dag_id
            dag = create_dag_from_nodes_edges_tasks(dag_data.get('info'), dag_data.get('nodes'), dag_data.get('edges'), dag_data.get('tasks'))
            print('create:', dag_id, dag)
            globals()[dag_id] = dag
            dag_dict[dag_id] = dag
    return dag_dict


#create_all()

'''
#if __name__ == '__main__':
#    dag.clear(dag_run_state=State.NONE)
#    dag.run()
'''

'''
BASE_NAME = 'dynamic-dag-'

dags = []

#Variable.set('dynamic-dags', ['a1','a2'], True)
#Variable.delete('dynamic-dag-a1')
#Variable.set('dynamic-dag-a1', {"nodes":{"children": [{"id": "t1", "value": {"label": "t1", "labelStyle": "fill:#000;", "rx": 5, "ry": 5, "style": "fill:#f0ede4;"}}], "id": None, "tooltip": "", "value": {"clusterLabelPos": "top", "label": None, "labelStyle": "fill:#000;", "rx": 5, "ry": 5, "style": "fill:CornflowerBlue"}},"tasks":{"t1": {"dag_id": "a1", "extra_links": [], "task_type": "BashOperator", "bash_command": "echo 1"},},"edges":[]}, True)

dags = Variable.get('dynamic-dags', [], True)

# 顺序构建所有DAG，注意构建效率！！！
for dag_id in dags:
    dag_data = Variable.get(BASE_NAME+dag_id, None, True)
    if dag_data:
        dag_data['info']['dag_id'] = dag_id
        dag = create_dag_from_nodes_edges_tasks(dag_data.get('info'), dag_data.get('nodes'), dag_data.get('edges'), dag_data.get('tasks'))
        print(dag)
        globals()[dag_id] = dag


#if __name__ == '__main__':
#    dag.clear(dag_run_state=State.NONE)
#    dag.run()
'''


'''
def prev_execution_dt(execution_date, **kwargs):
    weekday=execution_date.strftime('%A')
    print(weekday)
    if weekday == "Thursday":
        execution_dt_derived=execution_date - timedelta(hours=72)
        print(execution_dt_derived)
    else:
        execution_dt_derived=execution_date - timedelta(hours=24)
        print(execution_dt_derived)
    return execution_dt_derived

# lambda dt: dt + timedelta(days=1)
'''

