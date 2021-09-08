
import airflow

from airflow.jobs.scheduler_job import SchedulerJob

from airflow.utils.session import provide_session 
from typing import TYPE_CHECKING, Any, Iterable, List, NamedTuple, Optional, Tuple, Union
from typing import DefaultDict, Dict, Iterable, List, Optional, Set, Tuple
#from sqlalchemy.orm.session import Session
from sqlalchemy.orm.session import Session, make_transient
from airflow.ti_deps.dependencies_states import EXECUTION_STATES
from airflow.stats import Stats
from airflow.utils import timezone

# from airflow.jobs.scheduler_job.SchedulerJob.__get_concurrency_maps

@provide_session
def wrapper___get_concurrency_maps(
        self, states: List[str], session: Session = None
) -> Tuple[DefaultDict[str, int], DefaultDict[Tuple[str, str], int]]:
        """
        Get the concurrency maps.

        :param states: List of states to query for
        :type states: list[airflow.utils.state.State]
        :return: A map from (dag_id, task_id) to # of task instances and
         a map from (dag_id, task_id) to # of task instances in the given state list
        :rtype: tuple[dict[str, int], dict[tuple[str, str], int]]
        """
        ti_concurrency_query: List[Tuple[str, str, int]] = (
            session.query(TI.task_id, TI.dag_id, func.count('*'))
            .filter(TI.state.in_(states))
            .group_by(TI.task_id, TI.dag_id)
        ).all()
        dag_map: DefaultDict[str, int] = defaultdict(int)
        task_map: DefaultDict[Tuple[str, str], int] = defaultdict(int)
        for result in ti_concurrency_query:
            task_id, dag_id, count = result
            dag_map[dag_id] += count
            task_map[(dag_id, task_id)] = count
        return dag_map, task_map

SchedulerJob.__get_concurrency_maps = wrapper___get_concurrency_maps

from collections import defaultdict
from sqlalchemy.orm.session import Session
from typing import TYPE_CHECKING, Any, Iterable, List, NamedTuple, Optional, Tuple, Union
from airflow import models
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.utils.session import provide_session 
from sqlalchemy.orm import load_only, selectinload
from airflow.utils.sqlalchemy import is_lock_not_available_error, prohibit_commit, skip_locked, with_row_locks
from sqlalchemy import and_, func, not_, or_, tuple_
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
from airflow.jobs.scheduler_job import TI, DR, DM

if airflow.version.version > '2.1.3':
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
            #.filter(not_(DM.is_paused))
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


