
#########################################################
# support xml file
#
# from airflow.models import DagBag
# DagBag().process_file('xxxxxx', False, False)
# len(dagbag.dags), len(dagbag.import_errors)


import os
import zipfile
from datetime import datetime, timedelta


from airflow.models import DagBag

dagbag_process_file = DagBag.process_file

def wrapper_process_file(self, filepath, only_if_updated=True, safe_mode=True):
        """
        Given a path to a python module or zip file, this method imports
        the module and look for dag objects within it.
        """
        # if the source file no longer exists in the DB or in the filesystem,
        # return an empty list
        # todo: raise exception?
        if filepath is None or not os.path.isfile(filepath):
            return []

        try:
            # This failed before in what may have been a git sync
            # race condition
            file_last_changed_on_disk = datetime.fromtimestamp(os.path.getmtime(filepath))
            if (
                only_if_updated
                and filepath in self.file_last_changed
                and file_last_changed_on_disk == self.file_last_changed[filepath]
            ):
                return []
        except Exception as e:
            self.log.exception(e)
            return []

        if zipfile.is_zipfile(filepath):
            mods = self._load_modules_from_zip(filepath, safe_mode)
        elif conf.getboolean('core', 'LOAD_BPMNS') and os.path.splitext(filepath)[-1] == '.bpmn':
            mods = []
        #    from dynamic import dynamic_dags_variable
        #    dag, dag_id = dynamic_dags_variable.bpmn_file_to_dag_data(filepath)
        #    found_dags = [dag]
        #    self.file_last_changed[filepath] = file_last_changed_on_disk
        #    print('ddddddddddddddddddddddddddddddddddddd', filepath, found_dags)
        #    #self.log.info('ddddddddddddddddddddddddddddd %s', filepath, found_dags)
        #    return found_dags
            from dynamic import dynamic_dags_variable
            dag, dag_id = dynamic_dags_variable.bpmn_file_to_dag_data(filepath)
            class M():
                __dict__ = {dag_id: dag}
                pass
            m = M()
            m.__file__ = filepath
            dag, dag_id = dynamic_dags_variable.bpmn_file_to_dag_data(filepath)
            mods = [m,]
        else:
            mods = self._load_modules_from_file(filepath, safe_mode)

        found_dags = self._process_modules(filepath, mods, file_last_changed_on_disk)

        self.file_last_changed[filepath] = file_last_changed_on_disk
        #print('ddddddddddddddddddddddddddddddddddddd', filepath, found_dags)
        #self.log.info('ddddddddddddddddddddddddddddd %s', filepath, found_dags)
        return found_dags

DagBag.process_file = wrapper_process_file


import os
import zipfile
from typing import TYPE_CHECKING, Dict, Generator, List, Optional, Pattern, Union

from airflow.utils import file
from airflow.utils.file import find_path_from_directory, might_contain_dag, log

file_find_dag_file_paths = file.find_dag_file_paths

def wrapper_find_dag_file_paths(directory: Union[str, "pathlib.Path"], safe_mode: bool) -> List[str]:
    """Finds file paths of all DAG files."""
    file_paths = []

    for file_path in find_path_from_directory(str(directory), ".airflowignore"):
        try:
            if not os.path.isfile(file_path):
                continue
            _, file_ext = os.path.splitext(os.path.split(file_path)[-1])
            if file_ext != '.py' and not zipfile.is_zipfile(file_path) and (conf.getboolean('core', 'LOAD_BPMNS') and file_ext != '.bpmn' or not conf.getboolean('core', 'LOAD_BPMNS')) :
                continue
            if not might_contain_dag(file_path, safe_mode):
                continue

            file_paths.append(file_path)
        except Exception:
            log.exception("Error while examining %s", file_path)

    return file_paths

file.find_dag_file_paths = wrapper_find_dag_file_paths
#########################################################


'''
from airflow.www import app

func = app.create_app

def create_app_wrapper(config=None, testing=False, app_name="Airflow"):
    flask_app = func(config, testing, app_name)

    import jinja2
    flask_app.jinja_loader = jinja2.ChoiceLoader([
            jinja2.FileSystemLoader('/var/opt/bigdata/airflow-dag-creation-manager-plugin.git/templates'),
            #jinja2.FileSystemLoader('/var/opt/bigdata/airflow.git/airflow/www/templates'),
            flask_app.jinja_loader
        ])

    return flask_app

app.create_app = create_app_wrapper
'''


'''
# TODO: [Previous line repeated xxx more times]

# flask jinja templates explain

from flask.templating import DispatchingJinjaLoader

func = DispatchingJinjaLoader.get_source

def get_source_wrapper(self, environment, template):
        self.app.config["EXPLAIN_TEMPLATE_LOADING"] = False
        if self.app.config["EXPLAIN_TEMPLATE_LOADING"]:
            return self._get_source_explained(environment, template)
        return self._get_source_fast(environment, template)

DispatchingJinjaLoader.get_source = get_source_wrapper
'''


'''
# process name

from airflow.executors.local_executor import LocalWorkerBase, QueuedLocalWorker, LocalWorker
import setproctitle

func = LocalWorkerBase.__init__

def __init__unlimit_wrapper(self, result_queue: 'Queue[TaskInstanceStateType]'):
    super(LocalWorkerBase, self).__init__(target=self.do_work) #TODO:name
    self.daemon: bool = True
    self.result_queue: 'Queue[TaskInstanceStateType]' = result_queue
    setproctitle.setproctitle('[python airflow local executor]')

LocalWorkerBase.__init__ = __init__unlimit_wrapper
'''


'''
# patch airflow/utils/cli_action_loggers.py

from airflow.utils import cli_action_loggers

cli_action_loggers_default_action_log = cli_action_loggers.default_action_log
def patch_default_action_log(log, **_):
    return
cli_action_loggers.default_action_log = patch_default_action_log


# patch airflow/cli/commands/db_command.py

from airflow.cli.commands import db_command

db_command_initdb = db_command.initdb
def patch_initdb(args):
    # TODO create database
    return db_command_initdb(args)
db_command.initdb = patch_initdb


db_command_shell = db_command.shell
#@cli_utils.action_logging
def shell(args):
    # TODO
    return db_command_shell
db_command.shell = patch_shell
'''


'''
from airflow.utils import db

def wrapper_resetdb():
    """Clear out the database"""
    log.info("Dropping tables that exist")

    connection = settings.engine.connect()

    drop_airflow_models(connection)
    drop_flask_models(connection)

    #kube = Table('kube_worker_uuid', Base.metadata)
    #kube.drop(settings.engine, checkfirst=True)

    initdb()

db.resetdb = wrapper_resetdb
'''


'''
# TODO:xxx
# write log to es

from airflow.providers.elasticsearch.log.es_task_handler import ElasticsearchTaskHandler

def wrapper_emit(self, record):
        if self.write_stdout:
            self.formatter.format(record)
            if self.handler is not None:
                self.handler.emit(record)
        else:
            super(ElasticsearchTaskHandler, self).emit(record)

ElasticsearchTaskHandler.emit = wrapper_emit

def wrapper_flush(self):
        if self.handler is not None:
            self.handler.flush()

ElasticsearchTaskHandler.flush = wrapper_flush
'''


import airflow.version

# dot

from airflow.utils import dot_renderer
#pass


#

from setproctitle import setproctitle

from airflow.utils import serve_logs

import os

import flask

from airflow.configuration import conf

def wrapper_serve_logs():
    """Serves logs generated by Worker"""
    print("Starting flask")
    setproctitle("airflow serve-logs")
    flask_app = serve_logs.flask_app()
    '''log_directory = os.path.expanduser(conf.get('logging', 'BASE_LOG_FOLDER'))

    @flask_app.route('/log/<path:filename>')
    def serve_logs_view(filename):  # pylint: disable=unused-variable
        return flask.send_from_directory(
            log_directory, filename, mimetype="application/json", as_attachment=False
        )'''

    worker_log_server_port = conf.getint('celery', 'WORKER_LOG_SERVER_PORT')
    #flask_app.run(host='0.0.0.0', port=worker_log_server_port)
    from gunicorn.workers.ggevent import PyWSGIServer
    server = PyWSGIServer(('0.0.0.0', worker_log_server_port), flask_app)
    server.serve_forever()

#serve_logs.serve_logs = wrapper_serve_logs



from airflow.security import permissions

permissions.RESOURCE_DAG_ROLE = "DAGr"

permissions_resource_name_for_dag = permissions.resource_name_for_dag

def wrapper_resource_name_for_dag(dag_id):
    if dag_id == permissions.RESOURCE_DAG_ROLE:
        return dag_id

    return permissions_resource_name_for_dag(dag_id)

permissions.resource_name_for_dag = wrapper_resource_name_for_dag



from . import job

from . import access



from flask import current_app, g, request

from airflow.www import views

views_add_user_permissions_to_dag = views.add_user_permissions_to_dag

def wrapper_add_user_permissions_to_dag(sender, template, context, **extra):
    # TODO: only dag_id=xxx not list
    if 'dag' in context:
        dag = context['dag']
        if current_app.appbuilder.sm.has_access(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_ROLE):
            dag.can_trigger = True
            dag.can_edit = True
            dag.can_delete = True
        else:
            views_add_user_permissions_to_dag(sender, template, context, extra)

views.add_user_permissions_to_dag = wrapper_add_user_permissions_to_dag

from flask import before_render_template

before_render_template.connect(wrapper_add_user_permissions_to_dag)



from airflow.www.security import AirflowSecurityManager
#reload(AirflowSecurityManager)


from typing import Callable, Optional, Sequence, Tuple, TypeVar, cast

AirflowSecurityManager_can_access_some_dags = AirflowSecurityManager.can_access_some_dags

def wrapper_can_access_some_dags(self, action: str, dag_id: Optional[str] = None) -> bool:
        """Checks if user has read or write access to some dags."""
        if dag_id and dag_id != '~':
            return self.has_access(action, permissions.resource_name_for_dag(dag_id))

        user = g.user
        if action == permissions.ACTION_CAN_READ:
            return dag_id in self.get_readable_dags(user)
            #return any(self.get_readable_dags(user))
        return dag_id in self.get_editable_dags(user)
        #return any(self.get_editable_dags(user))

AirflowSecurityManager.can_access_some_dags = wrapper_can_access_some_dags



from airflow.utils.session import provide_session 
from flask_appbuilder.security.sqla.models import PermissionView, Role, User
from sqlalchemy.orm import joinedload
from airflow.models import DagBag, DagModel


AirflowSecurityManager_is_dag_resource = AirflowSecurityManager.is_dag_resource

def wrapper_is_dag_resource(self, resource_name):
        """Determines if a resource belongs to a DAG or all DAGs."""
        if resource_name == permissions.RESOURCE_DAG or resource_name == permissions.RESOURCE_DAG_ROLE:
            return True
        return resource_name.startswith(permissions.RESOURCE_DAG_PREFIX)

AirflowSecurityManager.is_dag_resource = wrapper_is_dag_resource

AirflowSecurityManager_get_accessible_dags = AirflowSecurityManager.get_accessible_dags

@provide_session
def wrapper_get_accessible_dags(self, user_actions, user, session=None):
        """Generic function to get readable or writable DAGs for user."""
        if user.is_anonymous:
            roles = self.get_user_roles(user)
        else:
            user_query = (
                session.query(User)
                .options(
                    joinedload(User.roles)
                    .subqueryload(Role.permissions)
                    .options(joinedload(PermissionView.permission), joinedload(PermissionView.view_menu))
                )
                .filter(User.id == user.id)
                .first()
            )
            roles = user_query.roles

        resources = set()
        for role in roles:
            for permission in role.permissions:
                action = permission.permission.name
                if action not in user_actions:
                    continue

                resource = permission.view_menu.name
                if resource == permissions.RESOURCE_DAG:
                    return session.query(DagModel)

                if resource.startswith(permissions.RESOURCE_DAG_PREFIX):
                    resources.add(resource[len(permissions.RESOURCE_DAG_PREFIX) :])
                # TODO:
                elif resource == permissions.RESOURCE_DAG_ROLE:
                    from sqlalchemy import or_
                    # DAGr : dag_id or owner
                    dags = session.query(DagModel).filter(or_(DagModel.owners.like('%'+role.name+'%'), DagModel.dag_id.like(role.name+'-%')))
                    ids = {dag.dag_id for dag in dags}
                    for d_id in ids:
                        resources.add(d_id)
                else:
                    resources.add(resource)

            # TODO:
            if not role.permissions:
                    dags = session.query(DagModel).filter(DagModel.dag_id.like(role.name+'-%'))
                    ids = {dag.dag_id for dag in dags}
                    for d_id in ids:
                        resources.add(d_id)

        return session.query(DagModel).filter(DagModel.dag_id.in_(resources))

AirflowSecurityManager.get_accessible_dags = wrapper_get_accessible_dags



'''
from typing import Callable, Optional, Sequence, Tuple, TypeVar, cast

AirflowSecurityManager_can_access_some_dags = AirflowSecurityManager.can_access_some_dags

def wrapper_can_access_some_dags(self, action: str, dag_id: Optional[str] = None) -> bool:
    print('ppppppppppp')
    pass

AirflowSecurityManager.can_access_some_dags = wrapper_can_access_some_dags
'''



from typing import Dict, List, Optional, Sequence, Set, Tuple

AirflowSecurityManager_check_authorization = AirflowSecurityManager.check_authorization

def wrapper_check_authorization(
        self, perms: Optional[Sequence[Tuple[str, str]]] = None, dag_id: Optional[str] = None
    ) -> bool:
        """Checks that the logged in user has the specified permissions."""
        if not perms:
            return True

        for perm in perms:
            if perm in (
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            ):
                can_access_all_dags = self.has_access(*perm)
                if can_access_all_dags:
                    continue

                action = perm[0]
                if self.can_access_some_dags(action, dag_id):
                    continue
                return False

            # TODO: xxx
            elif perm in ( (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_ROLE), (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_ROLE), ):
                action = perm[0]
                user = g.user
                if action == permissions.ACTION_CAN_READ:
                    if dag_id in self.get_readable_dag_ids(user):
                        continue
                    return False
                if dag_id in self.get_editable_dags(user):
                    continue
                return False
                #if self.can_access_some_dags(action, dag_id):
                #    continue
                #return False

            elif not self.has_access(*perm):
                return False

        return True

AirflowSecurityManager.check_authorization = wrapper_check_authorization


#from sqlalchemy.event import listen

#listen(User, 'after_insert', lambda mapper, connection, target: sync_profiles(action="insert", target=target))
#listen(User, 'after_delete', lambda mapper, connection, target: sync_profiles(action="delete", target=target))


# views.blocked

from airflow.www.views import Airflow

from flask_appbuilder import expose
from airflow.www import auth
from airflow.www import auth, utils as wwwutils
from airflow.utils.session import provide_session
#from airflow.security import permissions
from flask import current_app, g, request
from urllib.parse import unquote
import sqlalchemy as sqla
from airflow.utils.state import State
from airflow.models.dagrun import DagRun, DagRunType
from typing import Callable, Optional, Sequence, Tuple, TypeVar, cast
from flask import current_app, flash, redirect, request, url_for



'''
@expose('/blocked', methods=['POST'])
@auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        ]
)
@provide_session
def wrapper_blocked(self, session=None):
        """Mark Dag Blocked."""
        allowed_dag_ids = current_app.appbuilder.sm.get_accessible_dag_ids(g.user)

        # Filter by post parameters
        selected_dag_ids = {unquote(dag_id) for dag_id in request.form.getlist('dagIds') if dag_id}

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        if not filter_dag_ids:
            return wwwutils.json_response([])

        # pylint: disable=comparison-with-callable
        dags = (
            session.query(DagRun.dag_id, sqla.func.count(DagRun.id))
            .filter(DagRun.state == State.RUNNING)
            .filter(DagRun.dag_id.in_(filter_dag_ids))
            .group_by(DagRun.dag_id)
        )
        # pylint: enable=comparison-with-callable

        payload = []
        for dag_id, active_dag_runs in dags:
            max_active_runs = 0
            dag = current_app.dag_bag.get_dag(dag_id)
            if dag:
                # TODO: Make max_active_runs a column so we can query for it directly
                max_active_runs = dag.max_active_runs
            payload.append(
                {
                    'dag_id': dag_id,
                    'active_dag_run': active_dag_runs,
                    'max_active_runs': max_active_runs,
                }
            )
        return wwwutils.json_response(payload)

Airflow.blocked = wrapper_blocked


# views.trigger

from airflow.www.decorators import action_logging
from airflow.www.views import get_safe_url
from airflow import models
from airflow.utils import json as utils_json, timezone, yaml
from flask import flash, redirect

@expose('/trigger', methods=['POST', 'GET'])
@auth.has_access(
        [
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG_RUN),
        ]
)
@action_logging
@provide_session
def wrapper_trigger(self, session=None):
        """Triggers DAG Run."""
        dag_id = request.values.get('dag_id')
        origin = get_safe_url(request.values.get('origin'))
        request_conf = request.values.get('conf')

        if request.method == 'GET':
            # Populate conf textarea with conf requests parameter, or dag.params
            default_conf = ''

            dag = current_app.dag_bag.get_dag(dag_id)
            doc_md = wwwutils.wrapped_markdown(getattr(dag, 'doc_md', None))

            if request_conf:
                default_conf = request_conf
            else:
                try:
                    default_conf = json.dumps(dag.params, indent=4)
                except TypeError:
                    flash("Could not pre-populate conf field due to non-JSON-serializable data-types")
            return self.render_template(
                'airflow/trigger.html', dag_id=dag_id, origin=origin, conf=default_conf, doc_md=doc_md
            )

        dag_orm = session.query(models.DagModel).filter(models.DagModel.dag_id == dag_id).first()
        if not dag_orm:
            flash(f"Cannot find dag {dag_id}")
            return redirect(origin)

        execution_date = timezone.utcnow()

        dr = DagRun.find(dag_id=dag_id, execution_date=execution_date, run_type=DagRunType.MANUAL)
        if dr:
            flash(f"This run_id {dr.run_id} already exists")  # noqa
            return redirect(origin)

        run_conf = {}
        if request_conf:
            try:
                run_conf = json.loads(request_conf)
                if not isinstance(run_conf, dict):
                    flash("Invalid JSON configuration, must be a dict", "error")
                    return self.render_template(
                        'airflow/trigger.html', dag_id=dag_id, origin=origin, conf=request_conf
                    )
            except json.decoder.JSONDecodeError:
                flash("Invalid JSON configuration, not parseable", "error")
                return self.render_template(
                    'airflow/trigger.html', dag_id=dag_id, origin=origin, conf=request_conf
                )

        dag = current_app.dag_bag.get_dag(dag_id)
        dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=execution_date,
            state=State.RUNNING,
            conf=run_conf,
            external_trigger=True,
            dag_hash=current_app.dag_bag.dags_hash.get(dag_id),
        )

        flash(f"Triggered {dag_id}, it should start any moment now.")
        return redirect(origin)

Airflow.trigger = wrapper_trigger
'''


# dag_run

from sqlalchemy.orm.session import Session
from typing import TYPE_CHECKING, Any, Iterable, List, NamedTuple, Optional, Tuple, Union
from airflow import settings
from airflow.utils.sqlalchemy import UtcDateTime, nulls_first, skip_locked, with_row_locks
from sqlalchemy.sql import expression
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    PickleType,
    String,
    UniqueConstraint,
    and_,
    func,
    or_,
)

if airflow.version.version > '2.1.2':
    from airflow.models.dagrun import DagRun
    from airflow.utils.state import DagRunState

    @classmethod
    def wrapper_next_dagruns_to_examine(
        cls,
        state: DagRunState,
        session: Session,
        max_number: Optional[int] = None,
    ):
        """
        Return the next DagRuns that the scheduler should attempt to schedule.
        This will return zero or more DagRun rows that are row-level-locked with a "SELECT ... FOR UPDATE"
        query, you should ensure that any scheduling decisions are made in a single transaction -- as soon as
        the transaction is committed it will be unlocked.
        :rtype: list[airflow.models.DagRun]
        """
        from airflow.models.dag import DagModel

        if max_number is None:
            max_number = cls.DEFAULT_DAGRUNS_TO_EXAMINE

        # TODO: Bake this query, it is run _A lot_
        query = (
            session.query(cls)
            .filter(cls.state == state, cls.run_type != DagRunType.BACKFILL_JOB)
            .join(
                DagModel,
                DagModel.dag_id == cls.dag_id,
            )
            .filter(
                #DagModel.is_paused == expression.false(), # TODO: 已暂停但RUNNING的DAG不允许？
                DagModel.is_active == expression.true(),
            )
            .order_by(
                nulls_first(cls.last_scheduling_decision, session=session),
                cls.execution_date,
            )
        )

        if not settings.ALLOW_FUTURE_EXEC_DATES:
            query = query.filter(DagRun.execution_date <= func.now())

        return with_row_locks(
            query.limit(max_number), of=cls, session=session, **skip_locked(session=session)
        )

    DagRun.next_dagruns_to_examine = wrapper_next_dagruns_to_examine


'''
# job

from airflow.jobs.scheduler_job import SchedulerJob

@provide_session
def wrapper__executable_task_instances_to_queued(self, max_tis: int, session: Session = None) -> List[TI]:
        """
        Finds TIs that are ready for execution with respect to pool limits,
        dag max_active_tasks, executor state, and priority.
        :param max_tis: Maximum number of TIs to queue in this loop.
        :type max_tis: int
        :return: list[airflow.models.TaskInstance]
        """
        executable_tis: List[TI] = []

        # Get the pool settings. We get a lock on the pool rows, treating this as a "critical section"
        # Throws an exception if lock cannot be obtained, rather than blocking
        pools = models.Pool.slots_stats(lock_rows=True, session=session)

        # If the pools are full, there is no point doing anything!
        # If _somehow_ the pool is overfull, don't let the limit go negative - it breaks SQL
        pool_slots_free = max(0, sum(pool['open'] for pool in pools.values()))

        if pool_slots_free == 0:
            self.log.debug("All pools are full!")
            return executable_tis

        max_tis = min(max_tis, pool_slots_free)

        # Get all task instances associated with scheduled
        # DagRuns which are not backfilled, in the given states,
        # and the dag is not paused
        query = (
            session.query(TI)
            .outerjoin(TI.dag_run)
            .filter(or_(DR.run_id.is_(None), DR.run_type != DagRunType.BACKFILL_JOB))
            .join(TI.dag_model)
            #.filter(not_(DM.is_paused)) # TODO: 已暂停但RUNNING的DAG不允许？
            .filter(TI.state == State.SCHEDULED)
            .options(selectinload('dag_model'))
            .order_by(-TI.priority_weight, TI.execution_date)
        )
        starved_pools = [pool_name for pool_name, stats in pools.items() if stats['open'] <= 0]
        if starved_pools:
            query = query.filter(not_(TI.pool.in_(starved_pools)))

        query = query.limit(max_tis)

        task_instances_to_examine: List[TI] = with_row_locks(
            query,
            of=TI,
            session=session,
            **skip_locked(session=session),
        ).all()
        # TODO[HA]: This was wrong before anyway, as it only looked at a sub-set of dags, not everything.
        # Stats.gauge('scheduler.tasks.pending', len(task_instances_to_examine))

        if len(task_instances_to_examine) == 0:
            self.log.debug("No tasks to consider for execution.")
            return executable_tis

        # Put one task instance on each line
        task_instance_str = "\n\t".join(repr(x) for x in task_instances_to_examine)
        self.log.info("%s tasks up for execution:\n\t%s", len(task_instances_to_examine), task_instance_str)

        pool_to_task_instances: DefaultDict[str, List[models.Pool]] = defaultdict(list)
        for task_instance in task_instances_to_examine:
            pool_to_task_instances[task_instance.pool].append(task_instance)

        # dag_id to # of running tasks and (dag_id, task_id) to # of running tasks.
        dag_max_active_tasks_map: DefaultDict[str, int]
        task_concurrency_map: DefaultDict[Tuple[str, str], int]
        dag_max_active_tasks_map, task_concurrency_map = self.__get_concurrency_maps(
            states=list(EXECUTION_STATES), session=session
        )

        num_tasks_in_executor = 0
        # Number of tasks that cannot be scheduled because of no open slot in pool
        num_starving_tasks_total = 0

        # Go through each pool, and queue up a task for execution if there are
        # any open slots in the pool.
        # pylint: disable=too-many-nested-blocks
        for pool, task_instances in pool_to_task_instances.items():
            pool_name = pool
            if pool not in pools:
                self.log.warning("Tasks using non-existent pool '%s' will not be scheduled", pool)
                continue

            open_slots = pools[pool]["open"]

            num_ready = len(task_instances)
            self.log.info(
                "Figuring out tasks to run in Pool(name=%s) with %s open slots "
                "and %s task instances ready to be queued",
                pool,
                open_slots,
                num_ready,
            )

            priority_sorted_task_instances = sorted(
                task_instances, key=lambda ti: (-ti.priority_weight, ti.execution_date)
            )

            num_starving_tasks = 0
            for current_index, task_instance in enumerate(priority_sorted_task_instances):
                if open_slots <= 0:
                    self.log.info("Not scheduling since there are %s open slots in pool %s", open_slots, pool)
                    # Can't schedule any more since there are no more open slots.
                    num_unhandled = len(priority_sorted_task_instances) - current_index
                    num_starving_tasks += num_unhandled
                    num_starving_tasks_total += num_unhandled
                    break

                # Check to make sure that the task max_active_tasks of the DAG hasn't been
                # reached.
                dag_id = task_instance.dag_id

                current_max_active_tasks_per_dag = dag_max_active_tasks_map[dag_id]
                max_active_tasks_per_dag_limit = task_instance.dag_model.max_active_tasks
                self.log.info(
                    "DAG %s has %s/%s running and queued tasks",
                    dag_id,
                    current_max_active_tasks_per_dag,
                    max_active_tasks_per_dag_limit,
                )
                if current_max_active_tasks_per_dag >= max_active_tasks_per_dag_limit:
                    self.log.info(
                        "Not executing %s since the number of tasks running or queued "
                        "from DAG %s is >= to the DAG's max_active_tasks limit of %s",
                        task_instance,
                        dag_id,
                        max_active_tasks_per_dag_limit,
                    )
                    continue

                task_concurrency_limit: Optional[int] = None
                if task_instance.dag_model.has_task_concurrency_limits:
                    # Many dags don't have a task_concurrency, so where we can avoid loading the full
                    # serialized DAG the better.
                    serialized_dag = self.dagbag.get_dag(dag_id, session=session)
                    if serialized_dag.has_task(task_instance.task_id):
                        task_concurrency_limit = serialized_dag.get_task(
                            task_instance.task_id
                        ).task_concurrency

                    if task_concurrency_limit is not None:
                        current_task_concurrency = task_concurrency_map[
                            (task_instance.dag_id, task_instance.task_id)
                        ]

                        if current_task_concurrency >= task_concurrency_limit:
                            self.log.info(
                                "Not executing %s since the task concurrency for"
                                " this task has been reached.",
                                task_instance,
                            )
                            continue

                if task_instance.pool_slots > open_slots:
                    self.log.info(
                        "Not executing %s since it requires %s slots "
                        "but there are %s open slots in the pool %s.",
                        task_instance,
                        task_instance.pool_slots,
                        open_slots,
                        pool,
                    )
                    num_starving_tasks += 1
                    num_starving_tasks_total += 1
                    # Though we can execute tasks with lower priority if there's enough room
                    continue

                executable_tis.append(task_instance)
                open_slots -= task_instance.pool_slots
                dag_max_active_tasks_map[dag_id] += 1
                task_concurrency_map[(task_instance.dag_id, task_instance.task_id)] += 1

            Stats.gauge(f'pool.starving_tasks.{pool_name}', num_starving_tasks)

        Stats.gauge('scheduler.tasks.starving', num_starving_tasks_total)
        Stats.gauge('scheduler.tasks.running', num_tasks_in_executor)
        Stats.gauge('scheduler.tasks.executable', len(executable_tis))

        task_instance_str = "\n\t".join(repr(x) for x in executable_tis)
        self.log.info("Setting the following tasks to queued state:\n\t%s", task_instance_str)
        if len(executable_tis) > 0:
            # set TIs to queued state
            filter_for_tis = TI.filter_for_tis(executable_tis)
            session.query(TI).filter(filter_for_tis).update(
                # TODO[ha]: should we use func.now()? How does that work with DB timezone
                # on mysql when it's not UTC?
                {TI.state: State.QUEUED, TI.queued_dttm: timezone.utcnow(), TI.queued_by_job_id: self.id},
                synchronize_session=False,
            )

        for ti in executable_tis:
            make_transient(ti)
        return executable_tis

SchedulerJob._executable_task_instances_to_queued = wrapper__executable_task_instances_to_queued
'''


from flask_appbuilder.widgets import ShowWidget
class VariableWidget(ShowWidget):
    template = 'widgets/show.html'


# can_add

from airflow.www.views import CustomViewMenuModelView, CustomPermissionViewModelView, VariableModelView
from airflow.security import permissions

VariableModelView.method_permission_name['show'] = 'read'

VariableModelView.show_widget = VariableWidget

CustomViewMenuModelView.base_permissions = [
        permissions.ACTION_CAN_READ,
        'can_add',
    ]

from flask import Markup

def wrapper_hidden_field_formatter(self):
        val = self

        if val:
            return val
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

VariableModelView.hidden_field_formatter = wrapper_hidden_field_formatter

CustomPermissionViewModelView.base_permissions = [
        permissions.ACTION_CAN_READ,
        #permissions.ACTION_CAN_CREATE,
        #'can_read'
        'can_add',
        'can_edit',
    ]



# DAG

if airflow.version.version < '2.1.3':

    from airflow.models.dag import DAG

    from datetime import datetime, timedelta
    from airflow.models.taskinstance import Context, TaskInstance, TaskInstanceKey, clear_task_instances

    dag_get_task_instances_before = DAG.get_task_instances_before

    @provide_session
    def wrapper_get_task_instances_before(
        self,
        base_date: datetime,
        num: int,
        *,
        session: Session,
    ) -> List[TaskInstance]:
        """Get ``num`` task instances before (including) ``base_date``.

        The returned list may contain exactly ``num`` task instances. It can
        have less if there are less than ``num`` scheduled DAG runs before
        ``base_date``, or more if there are manual task runs between the
        requested period, which does not count toward ``num``.
        """
        min_date = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == self.dag_id,
                DagRun.execution_date <= base_date,
                DagRun.run_type != DagRunType.MANUAL,
            )
            .order_by(DagRun.execution_date.desc())
            .offset(num)
            .first()
            .execution_date
        )
        if min_date is None:
            min_date = timezone.utc_epoch()
        return self.get_task_instances(start_date=min_date, end_date=base_date, session=session)

    DAG.get_task_instances_before = wrapper_get_task_instances_before 




'''
from airflow.utils import cli

cli_get_dag = cli.get_dag

def wrapper_get_dag(subdir: Optional[str], dag_id: str) -> "DAG":
    """Returns DAG of a given dag_id"""
    from airflow.models import DagBag
    from airflow.exceptions import AirflowException
    process_subdir = cli.process_subdir

    print('read_dags_from_db=True')
    dagbag = DagBag(process_subdir(subdir))
    if dag_id not in dagbag.dags:
        dag = DagBag(process_subdir(subdir), read_dags_from_db=True).get_dag(dag_id)
        if not dag:
            raise AirflowException(
            'dag_id could not be found: {}. Either the dag did not exist or it failed to '
            'parse.'.format(dag_id)
            )
        else:
            return dag
    return dagbag.dags[dag_id]

cli.get_dag = wrapper_get_dag
'''







