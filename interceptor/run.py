#   Copyright 2018 getcarrier.io
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from interceptor import ssl_support, constants as c

ssl_support.init(c.SSL_CERTS)

import signal
from time import sleep, mktime
from traceback import format_exc
from typing import List, Optional, Union, Iterable

import boto3
import requests
from arbiter import Minion

from interceptor.containers_backend import KubernetesClient, DockerClient, Job, KubernetesJob
from interceptor.jobs_wrapper import JobsWrapper
from interceptor.lambda_executor import LambdaExecutor
from interceptor.logger import logger, get_centry_logger
from interceptor.post_processor import PostProcessor

from interceptor.utils import build_api_url

app = Minion(
    host=c.RABBIT_HOST, port=c.RABBIT_PORT,
    user=c.RABBIT_USER, password=c.RABBIT_PASSWORD,
    queue=c.QUEUE_NAME, vhost=c.VHOST,
    use_ssl=c.RABBIT_USE_SSL, ssl_verify=c.RABBIT_SSL_VERIFY
)

stop_task = False


def sigterm_handler(signal, frame):
    global stop_task
    stop_task = True


signal.signal(signal.SIGTERM, sigterm_handler)


@app.task("terminate_gcp_instances")
def terminate_gcp_instances(
        service_account_info: dict, project: str, zone: str, instances: List[str]
):
    from google.cloud import compute_v1
    from google.oauth2.service_account import Credentials
    # https://cloud.google.com/compute/docs/reference/rest/v1/instances/delete
    try:
        credentials = Credentials.from_service_account_info(service_account_info)
        instance_client = compute_v1.InstancesClient(credentials=credentials)
        for instance_name in instances:
            instance_client.delete(
                project=project,
                zone=zone,
                instance=instance_name
            )
    except Exception:
        logger.error(format_exc())
        logger.info("Failed to terminate GCP instances")


@app.task(name="terminate_ec2_instances")
def terminate_ec2_instances(
        aws_access_key_id, aws_secret_access_key, region_name, fleet_id, launch_template_id
):
    try:
        ec2 = boto3.client('ec2', aws_access_key_id=aws_access_key_id,
                           aws_secret_access_key=aws_secret_access_key,
                           region_name=region_name)
        response = ec2.delete_fleets(
            FleetIds=[
                fleet_id,
            ],
            TerminateInstances=True
        )
        logger.info(response)
        response = ec2.delete_launch_template(
            LaunchTemplateId=launch_template_id
        )
        logger.info(response)
        return "Done"
    except Exception:
        logger.error(format_exc())
        logger.info("Failed to terminate AWS ec2 instances")
        return "Failed"


@app.task(name="post_process")
def post_process(
        galloper_url: str,
        project_id: int,
        report_id: int,
        bucket: str,
        build_id: str,
        token: Optional[str] = None,
        integration: Optional[list] = None,
        exec_params: dict | str | None = None,
        logger_stop_words: Iterable = tuple(),
        manual_run: bool = False,
        **kwargs
) -> str:
    pp = PostProcessor(
        galloper_url=galloper_url,
        project_id=project_id,
        report_id=report_id,
        build_id=build_id,
        bucket=bucket,
        token=token,
        integrations=integration,
        exec_params=exec_params,
        manual_run=manual_run,
        logger_stop_words=logger_stop_words
    )
    if kwargs.get('skip'):
        return 'Done'
    try:
        global stop_task
        job: Job = pp.results_post_processing()
        # params = {'galloper_url': galloper_url, 'token': token,
        #           'report_id': report_id, 'project_id': project_id}
        while not job.is_finished():
            sleep(10)
            try:
                job.log_status([])
                # job.send_resource_usage(
                #     job_type='post_process',
                #     params=params,
                #     time_to_sleep=10
                # )
            except:
                break
            if stop_task:
                stop_task = False
                exit(0)
        return "Done"
    except Exception:
        from interceptor.logger import logger as global_logger
        global_logger.info("Failed to run postprocessor")
        global_logger.info(format_exc())
        return "Failed"


@app.task(name="browsertime")
def browsertime(
        galloper_url, project_id, token, bucket, filename, url, view='1920x1080',
        tests='1', headers=None, browser="", *args, **kwargs
):
    try:
        if not headers:
            headers = {}
        client = DockerClient(logger)
        env_vars = {
            "galloper_url": galloper_url, "project_id": project_id,
            "token": token, "bucket": bucket,
            "filename": filename, "view": view, "tests": tests
        }
        cmd = url
        if headers:
            cmd += " -H "
            for key, value in headers.items():
                if key.strip().lower() not in c.STRIP_HEADERS:
                    cmd += f'"{key.strip()}:{value.strip()}" '
        if browser:
            cmd += f" -b {browser}"

        job = getattr(JobsWrapper, 'browsertime')(client, c.BROWSERTIME_CONTAINER, env_vars,
                                                  cmd)
        while job.status != "exited":
            logger.info(f"Executing: {c.BROWSERTIME_CONTAINER}")
            logger.info(f"Execution params: {cmd}")
            logger.info(f"Container {job.id} status {job.status}")
            try:
                job.reload()
            except:
                break
            sleep(10)
        return "Done"
    except Exception:
        logger.error(format_exc())
        logger.info("Failed to run browsertime task")
        return "Failed"


@app.task(name="execute_lambda")
def execute_lambda(task: dict, event: Union[dict, list],
                   logger_stop_words: Iterable = tuple(),
                   **kwargs) -> str:
    centry_logger = get_centry_logger(
        hostname=task.get('task_name'),
        labels={
            'task_id': task['task_id'],
            'project': task['project_id'],
            'task_result_id': task['task_result_id'],
        },
        stop_words=logger_stop_words
    )
    try:
        LambdaExecutor(
            task=task, event=event, logger=centry_logger,
            **kwargs
        ).execute_lambda()
        return "Done"
    except Exception as e:
        logger.error(format_exc())
        logger.info(f"Failed to execute {task['task_name']} lambda")
        logger.info(str(e))
        return "Failed"


@app.task(name="execute_kuber")
def execute_kuber(job_type, container, execution_params, job_name, kubernetes_settings: dict,
                  mode: str = 'default', logger_stop_words: Iterable = tuple(),
                  **kwargs
                  ):
    try:
        labels = {
            "project": execution_params['project_id'],
            "report_id": execution_params['report_id'],
        }
        if execution_params.get('build_id', None):
            labels["build_id"] = execution_params['build_id']
        centry_logger = get_centry_logger(
            hostname="interceptor",
            labels=labels,
            stop_words=logger_stop_words
        )
    except Exception as e:
        print(e)
        centry_logger = logger

    if not getattr(JobsWrapper, job_type):
        centry_logger.error("Job Type not found")
        return

    kubernetes_settings['mode'] = mode
    kubernetes_settings['logger'] = centry_logger
    client = KubernetesClient(**kubernetes_settings)
    try:
        job: KubernetesJob = getattr(JobsWrapper, job_type)(client, container,
                                                            execution_params, job_name)
    except Exception as exc:
        centry_logger.error(exc)
        return
    last_logs = []
    time_to_sleep = 10
    while not job.is_finished():
        sleep(time_to_sleep)
        global stop_task
        if stop_task:
            stop_task = False
            job.stop_job()
            return
        try:
            job.log_status(last_logs)
            # job.send_resource_usage(job_type=job_type, params=execution_params,
            #     time_to_sleep=time_to_sleep)
        except Exception as exc:
            centry_logger.warning(f"FETCHING LOGS FAILED {exc}")
    return "Done"


@app.task(name="execute")
def execute_job(job_type, container, execution_params, job_name,
                logger_stop_words: Iterable = tuple(),
                **kwargs):
    try:
        labels = {
            "project": execution_params['project_id'],
            "report_id": execution_params['report_id'],
        }
        if execution_params.get('build_id', None):
            labels["build_id"] = execution_params['build_id']
        centry_logger = get_centry_logger(
            hostname="interceptor",
            labels=labels,
            stop_words=logger_stop_words
        )
    except Exception as e:
        print(e)
        centry_logger = logger

    if not getattr(JobsWrapper, job_type):
        centry_logger.error("Job Type not found")
        return "Job Type not found"

    client = DockerClient(logger=centry_logger)
    centry_logger.info(f"Executing: {job_type} on {container} with name {job_name}")
    centry_logger.info(f"Execution params: {execution_params}")
    try:
        job: Job = getattr(JobsWrapper, job_type)(client, container, execution_params,
                                                  job_name)
    except Exception as e:
        centry_logger.error(f"Failed to run docker container {container}")
        return f"Failed to run docker container {container}"
    last_logs = []
    time_to_sleep = 10
    while not job.is_finished():
        sleep(time_to_sleep)
        global stop_task
        if stop_task:
            stop_task = False
            job.stop_job()
            centry_logger.info(f"Aborted: {job_type} on {container} with name {job_name}")
            exit(0)
        try:
            job.log_status(last_logs)
            # job.send_resource_usage(job_type=job_type, params=execution_params,
            #     time_to_sleep=time_to_sleep)
        except Exception as e:
            centry_logger.debug(format_exc())
            centry_logger.error(e)
            break
    return "Done"


def main():
    if c.QUEUE_NAME != "__internal":
        if c.PYLON_URL:
            rabbit_url = build_api_url('projects', 'rabbitmq', mode='administration')
            url = f"{c.PYLON_URL}{rabbit_url}/{c.VHOST}"
            data = {"name": c.QUEUE_NAME}
            headers = {'content-type': 'application/json'}
            if c.TOKEN:
                headers['Authorization'] = f'bearer {c.TOKEN}'
            try:
                requests.post(url, json=data, headers=headers, verify=c.SSL_VERIFY)
            except requests.exceptions.ConnectionError:
                print('FAILED TO REGISTER IN PYLON', url, data)
    app.run(workers=int(c.CPU_CORES))


if __name__ == '__main__':
    main()
