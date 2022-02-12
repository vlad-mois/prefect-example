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


with Flow('some-sqlite-flow') as flow:
    db = Parameter('db', '/tmp/prefect_example.db')
    source = Parameter('source', 'images')
    destination = Parameter('destination', 'results')
    processing = Parameter('processing', 'processing')
    project_id = Parameter('project_id', '77582')
    ver_project_id = Parameter('ver_project_id', '77585')

    query_move = StringFormatter('''
        INSERT INTO {processing} SELECT "{flow_run_id}", url FROM {source};
        DELETE FROM {source};
    ''')(source=source, processing=processing)
    move = SQLiteScript()(db, query_move)

    query_load = StringFormatter('SELECT url FROM {table} WHERE flow="{flow_run_id}"')(table=processing)
    rows = SQLiteQuery()(db, query_load, upstream_tasks=[move])

    with case(NotEqual()(rows, []), True):

        pool_conf = download_json('https://raw.githubusercontent.com/vlad-mois/prefect-example/main/pool.json')
        pool = tlk.create_pool(pool_conf, project_id=project_id, expiration=timedelta(days=1))

        tasks = FunctionTask(lambda row: {'input_values': {'image': row[0]}}).map(rows)

        tasks_upload = tlk.create_tasks(tasks, pool_id=pool, allow_defaults=True)
        pool_done = tlk.wait_pool(pool, open_pool=True, upstream_tasks=[tasks_upload])

        assignments = tlk.get_assignments(pool, status='SUBMITTED', upstream_tasks=[pool_done])

        ver_pool_conf = download_json('https://raw.githubusercontent.com/vlad-mois/prefect-example/main/ver_pool.json')
        ver_pool = tlk.create_pool(ver_pool_conf, project_id=ver_project_id, expiration=timedelta(days=1))

        ver_tasks = prepare_verification_tasks(assignments)

        ver_upload = tlk.create_tasks(ver_tasks, pool_id=ver_pool, allow_defaults=True)
        ver_done = tlk.wait_pool(ver_pool, open_pool=True, upstream_tasks=[ver_upload])

        to_aggregate = FunctionTask(lambda df: df.rename(columns={
            'INPUT:assignment_id': 'task',
            'OUTPUT:result': 'label',
            'ASSIGNMENT:worker_id': 'performer'})
        )(tlk.get_assignments_df(ver_pool, upstream_tasks=[ver_done]))

        aggregated = FunctionTask(MajorityVote().fit_predict)(to_aggregate)
        to_accept = FunctionTask(lambda agg: [a_id for a_id, res in agg.items() if res == 'Yes'])(
            aggregated)
        to_reject = FunctionTask(lambda agg: [a_id for a_id, res in agg.items() if res != 'Yes'])(
            aggregated)

        accepted = tlk.accept_assignment.map(to_accept, unmapped('Well done'))
        rejected = tlk.reject_assignment.map(to_reject, unmapped('Incorrect object'))

        results = select_results(accepted, assignments)
        query_save = StringFormatter('INSERT INTO {table} (image, link) VALUES (?, ?)')(table=destination)
        save = SQLiteQuery().map(unmapped(db), unmapped(query_save), data=results)

        to_relaunch = select_non_accepted_urls(accepted, assignments)
        query_relaunch = StringFormatter('INSERT INTO {table} (url) VALUES (?)')(table=source)
        relaunch = SQLiteQuery().map(unmapped(db), unmapped(query_relaunch), data=to_relaunch)

        query_clean = StringFormatter('DELETE FROM {table} WHERE flow="{flow_run_id}";')(table=processing)
        SQLiteScript()(db, query_clean, upstream_tasks=[save, relaunch, rejected])

flow.register(project_name='Some test project')
