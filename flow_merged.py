import pandas as pd

from datetime import timedelta
from typing import Dict, List, Tuple

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


def prepare_verification_tasks(assignments: List[Assignment]) -> List[TolokaTask]:
    return [TolokaTask(unavailable_for=[assignment.user_id],
                       input_values={'image': task.input_values['image'],
                                     'found_link': solution.output_values['found_link'],
                                     'assignment_id': assignment.id})
            for assignment in assignments
            for task, solution in zip(assignment.tasks, assignment.solutions)]


@task
def aggregate(assignments: pd.DataFrame) -> Dict[str, List[str]]:
    renamed = assignments.rename(columns={'INPUT:assignment_id': 'task',
                                          'OUTPUT:result': 'label',
                                          'ASSIGNMENT:worker_id': 'performer'})
    to_accept = []
    to_reject = []
    for assignment_id, result in MajorityVote().fit_predict(renamed).items():
        dst = to_accept if result == 'Yes' else to_reject
        dst.append(assignment_id)

    return {'to_accept': to_accept, 'to_reject': to_reject}


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
VER_POOL_CONF_URL = 'https://raw.githubusercontent.com/vlad-mois/prefect-example/main/ver_pool.json'


with Flow('some-sqlite-flow') as flow:
    project_id = Parameter('project_id', '78750')
    ver_project_id = Parameter('ver_project_id', '78751')

    query_move = StringFormatter(f'INSERT INTO {PROCESSING} SELECT "{{flow_run_id}}", url FROM {SOURCE};'
                                 f'DELETE FROM {SOURCE};')()
    move = SQLiteScript()(DB, query_move)

    query_load = StringFormatter(f'SELECT url FROM {PROCESSING} WHERE flow="{{flow_run_id}}"')()
    rows = SQLiteQuery()(DB, query_load, upstream_tasks=[move])

    pool = tlk.launch_new_pool(
        pool_conf=POOL_CONF_URL,
        data=rows,
        transformation=lambda data: [{'input_values': {'image': row[0]}} for row in data],
        project_id=project_id,
        expiration=timedelta(hours=6),
        allow_defaults=True,
    )
    assignments = tlk.get_assignments(pool, status='SUBMITTED')

    ver_pool = tlk.launch_new_pool(
        pool_conf=VER_POOL_CONF_URL,
        data=assignments,
        transformation=prepare_verification_tasks,
        project_id=ver_project_id,
        expiration=timedelta(hours=6),
        allow_defaults=True,
    )
    aggregated = aggregate(tlk.get_assignments_df(ver_pool))

    accepted = tlk.accept_assignment.map(aggregated['to_accept'], unmapped('Well done'))
    rejected = tlk.reject_assignment.map(aggregated['to_reject'], unmapped('Incorrect object'))

    results = select_results(accepted, assignments)
    save = SQLiteQuery(DB, f'INSERT INTO {DESTINATION} (image, link) VALUES (?, ?)').map(data=results)

    to_relaunch = select_non_accepted_urls(accepted, assignments)
    relaunch = SQLiteQuery(DB, f'INSERT INTO {SOURCE} (url) VALUES (?)').map(data=to_relaunch)

    query_clean = StringFormatter(f'DELETE FROM {PROCESSING} WHERE flow="{{flow_run_id}}";')()
    SQLiteScript(name='clean')(DB, query_clean, upstream_tasks=[save, relaunch, rejected])

flow.register(project_name='Some test project')
