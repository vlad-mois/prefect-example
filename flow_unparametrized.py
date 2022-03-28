from datetime import timedelta
from typing import List, Tuple

from prefect import Flow, task, unmapped
from prefect.core import Parameter
from prefect.tasks.control_flow.case import case
from prefect.tasks.core.function import FunctionTask
from prefect.tasks.core.operators import NotEqual
from prefect.tasks.database.sqlite import SQLiteQuery, SQLiteScript
from prefect.tasks.templates.strings import StringFormatter

from crowdkit.aggregation import MajorityVote
from toloka.client import Assignment, Task as TolokaTask

from toloka_prefect import operations as tlk
from toloka_prefect.helpers import download_json


@task
def prepare_verification_tasks(assignments: List[Assignment]) -> List[TolokaTask]:
    return [TolokaTask(unavailable_for=[assignment.user_id],
                       input_values={'image': task.input_values['image'],
                                     'found_link': solution.output_values['found_link'],
                                     'assignment_id': assignment.id})
            for assignment in assignments
            for task, solution in zip(assignment.tasks, assignment.solutions)]


@task
def select_results(accepted: List[str], assignments: List[Assignment]) -> List[Tuple[str, str]]:
    accepted = set(accepted)
    return [(task.input_values['image'], solution.output_values['found_link'])
            for assignment in assignments
            if assignment.id in accepted
            for task, solution in zip(assignment.tasks, assignment.solutions)]


@task
def select_non_accepted_urls(accepted: List[str], assignments: List[Assignment]) -> List[Tuple[str]]:
    accepted = set(accepted)
    urls_accepted = set()
    urls_unclear = set()

    for assignment in assignments:
        dst = urls_accepted if assignment.id in accepted else urls_unclear
        dst.update(task.input_values['image'] for task in assignment.tasks)

    return [(url,) for url in urls_unclear - urls_accepted]


DB = '/tmp/prefect_example.db'
SOURCE = 'images'
PROCESSING = 'processing'
DESTINATION = 'results'
POOL_CONF_URL = 'https://raw.githubusercontent.com/vlad-mois/prefect-example/main/pool.json'
VER_POOL_CONF_URL = 'https://raw.githubusercontent.com/vlad-mois/prefect-example/main/val_pool.json'


with Flow('some-sqlite-flow') as flow:
    project_id = Parameter('project_id', '78750')
    val_project_id = Parameter('val_project_id', '78751')

    query_move = StringFormatter(f'INSERT INTO {PROCESSING} SELECT "{{flow_run_id}}", url FROM {SOURCE};'
                                 f'DELETE FROM {SOURCE};')()
    move = SQLiteScript()(DB, query_move)

    query_load = StringFormatter(f'SELECT url FROM {PROCESSING} WHERE flow="{{flow_run_id}}"')()
    rows = SQLiteQuery()(DB, query_load, upstream_tasks=[move])

    with case(NotEqual()(rows, []), True):

        pool_conf = download_json(POOL_CONF_URL)
        pool = tlk.create_pool(pool_conf, project_id=project_id, expiration=timedelta(hours=6))

        tasks = FunctionTask(lambda row: {'input_values': {'image': row[0]}}).map(rows)

        tasks_upload = tlk.create_tasks(tasks, pool_id=pool, allow_defaults=True)
        pool_done = tlk.wait_pool(pool, open_pool=True, upstream_tasks=[tasks_upload])

        assignments = tlk.get_assignments(pool, status='SUBMITTED', upstream_tasks=[pool_done])

        val_pool_conf = download_json(VER_POOL_CONF_URL)
        val_pool = tlk.create_pool(val_pool_conf, project_id=val_project_id, expiration=timedelta(hours=6))

        val_tasks = prepare_verification_tasks(assignments)

        val_upload = tlk.create_tasks(val_tasks, pool_id=val_pool, allow_defaults=True)
        val_done = tlk.wait_pool(val_pool, open_pool=True, upstream_tasks=[val_upload])

        to_aggregate = FunctionTask(lambda df: df.rename(columns={
            'INPUT:assignment_id': 'task',
            'OUTPUT:result': 'label',
            'ASSIGNMENT:worker_id': 'performer'})
        )(tlk.get_assignments_df(val_pool, upstream_tasks=[val_done]))

        aggregated = FunctionTask(MajorityVote().fit_predict)(to_aggregate)
        to_accept = FunctionTask(lambda agg: [a_id for a_id, res in agg.items() if res == 'Yes'])(
            aggregated)
        to_reject = FunctionTask(lambda agg: [a_id for a_id, res in agg.items() if res != 'Yes'])(
            aggregated)

        accepted = tlk.accept_assignment.map(to_accept, unmapped('Well done'))
        rejected = tlk.reject_assignment.map(to_reject, unmapped('Incorrect object'))

        results = select_results(accepted, assignments)
        save = SQLiteQuery(DB, f'INSERT INTO {DESTINATION} (image, link) VALUES (?, ?)').map(data=results)

        to_relaunch = select_non_accepted_urls(accepted, assignments)
        relaunch = SQLiteQuery(DB, f'INSERT INTO {SOURCE} (url) VALUES (?)').map(data=to_relaunch)

        query_clean = StringFormatter(f'DELETE FROM {PROCESSING} WHERE flow="{{flow_run_id}}";')()
        SQLiteScript()(DB, query_clean, upstream_tasks=[save, relaunch, rejected])

flow.register(project_name='Some test project')
