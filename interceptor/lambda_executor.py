import json
import time
import re
import shutil
from json import dumps, loads
from pathlib import Path
from subprocess import Popen, PIPE
from time import sleep
from traceback import format_exc
from typing import Tuple, Union
from uuid import uuid4

import requests
from docker import DockerClient
from docker.errors import APIError
from docker.models.volumes import Volume
from docker.types import Mount

from interceptor.containers_backend import KubernetesClient
from interceptor.logger import logger as global_logger
from interceptor.utils import build_api_url
from interceptor import constants as c


class LambdaExecutor:

    def __init__(self, task: dict, event: Union[dict, list], galloper_url: str, token: str,
                 mode: str = 'default', logger=global_logger,
                 token_type: str = 'bearer', api_version: int = 1,
                 **kwargs):
        self.logger = logger

        self.task = task
        if isinstance(event, list):
            self.event = event[0]
        else:
            self.event = event
        self.galloper_url = galloper_url
        self.token = token
        self.mode = mode
        self.start_time = time.time()
        self.api_version = api_version
        self.api_headers = {
            'Content-Type': 'application/json',
            'Authorization': f'{token_type} {self.token}',
            'X-From': 'interceptor'
        }

        self.env_vars = loads(self.task.get("env_vars", "{}"))
        if self.task['task_name'] == "control_tower" and "cc_env_vars" in self.event:
            self.env_vars.update(self.event["cc_env_vars"])

        artifact_url_part = build_api_url('artifacts', 'artifact',
                                          mode=self.mode, api_version=self.api_version)
        zippath = self.task["zippath"]
        self.artifact_url = f'{self.galloper_url}{artifact_url_part}/' \
                            f'{self.task["project_id"]}/{zippath["bucket_name"]}/' \
                            f'{zippath["file_name"]}?integration_id={zippath["integration_id"]}&' \
                            f'is_local={zippath["is_local"]}'
        self.command = [f"{self.task['task_handler']}", dumps(self.event)]

        self.execution_params = None
        if self.event:
            value = self.event.get('execution_params', None)
            self.execution_params = loads(value) if value else value

    def execute_lambda(self):
        self.logger.info(f'task {self.task}')
        self.logger.info(f'event {self.event}')
        container_name = c.NAME_CONTAINER_MAPPING.get(self.task['runtime'])
        if not container_name:
            self.logger.error(f"Container {self.task['runtime']} is not found")
            raise Exception(f"Container {self.task['runtime']} is not found")

        integrations = self.event.get("integration") or self.event.get("integrations")

        try:
            cloud_settings = integrations["clouds"]["kubernetes"]
        except (TypeError, KeyError):
            log, stats = self.execute_in_docker(container_name)
        else:
            log, stats = self.execute_in_kubernetes(container_name, cloud_settings)

        if container_name == "lambda:python3.7":
            results = re.findall(r'({.+?})', log)[-1]
        else:
            # TODO: magic of 2 enters is very flaky, Need to think on how to workaround, probably with specific logging
            results = log.strip().split('\n\n')[-1]

        task_result_id = self.task["task_result_id"]
        try:
            task_status = "Done" if 200 <= int(json.loads(results).get('statusCode')) <= 299 else "Failed"
        except:
            task_status = "Failed"
        data = {
            'results': results,
            'log': log,
            'task_duration': time.time() - self.start_time,
            'task_status': task_status,
            'task_stats': stats
        }
        self.logger.info(f'Task body {data}')
        results_url = build_api_url('tasks', 'results', mode=self.mode, api_version=self.api_version)
        res = requests.put(
            f'{self.galloper_url}{results_url}/{self.task["project_id"]}?task_result_id={task_result_id}',
            headers=self.api_headers, data=dumps(data), verify=c.SSL_VERIFY
        )
        self.logger.info(f'Created task_results: {res.status_code, res.text}')

        # this was here to chain task executions, but is temporary disabled
        # if self.task.get("callback"):
        #     for each in self.event:
        #         each['result'] = results
        #     endpoint = f"/api/v1/task/{self.task['project_id']}/{self.task['callback']}?exec=True"
        #     self.task = requests.get(f"{self.galloper_url}/{endpoint}", headers=self.api_headers, verify=c.SSL_VERIFY).json()
        #     self.execute_lambda()
        self.logger.info('Done.')

    def execute_in_kubernetes(self, container_name: str, cloud_settings: dict):
        kubernetes_settings = {
            "host": cloud_settings["hostname"],
            "token": cloud_settings["k8s_token"],
            "namespace": cloud_settings["namespace"],
            "jobs_count": 1,
            "logger": self.logger,
            "secure_connection": cloud_settings["secure_connection"],
            "mode": self.mode
        }
        client = KubernetesClient(**kubernetes_settings)
        job = client.run_lambda(container_name, self.token, self.env_vars, self.artifact_url,
                                self.command)

        while not job.is_finished():
            sleep(5)
        logs = []
        job.log_status(logs)
        stats = {'kubernetes_stats': job.collect_resource_usage()}
        return "".join(logs), stats

    def remove_volume(self, volume, attempts: int = 3) -> None:
        for _ in range(attempts):
            sleep(1)
            try:
                volume.remove(force=True)
                self.logger.info(f'Volume removed {volume}')
                shutil.rmtree(volume._centry_path, ignore_errors=True)
                self.logger.info(f'Volume path cleared {volume._centry_path}')
                break
            except APIError:
                self.logger.info(f'Failed to remove volume. Sleeping for 1. Attempt {i + 1}/{attempts}')

        else:
            self.logger.warning(f'Failed to remove docker volume after {attempts} attempts')

    def execute_in_docker(self, container_name: str) -> Tuple[str, dict]:
        ATTEMPTS_TO_REMOVE_VOL = 3

        lambda_id = str(uuid4())
        client = DockerClient.from_env()

        self.download_artifact(lambda_id)
        volume = self.create_volume(client, lambda_id)
        mounts = [Mount(type='volume', source=volume.name, target='/var/task')]

        try:
            code_path = self.execution_params.get('code_path')
            if code_path:
                mounts.append(Mount(type='bind', source=code_path, target='/code'))
        except AttributeError:
            ...

        network_options = {}
        if c.LAMBDA_DOCKER_NETWORK:
            network_options["network"] = c.LAMBDA_DOCKER_NETWORK
        if c.LAMBDA_DOCKER_NETWORK_MODE:
            network_options["network_mode"] = c.LAMBDA_DOCKER_NETWORK_MODE

        nano_cpus = int(float(self.env_vars["cpu_cores"]) * c.CPU_MULTIPLIER) if self.env_vars.get(
            "cpu_cores") else c.CONTAINER_CPU_QUOTA
        mem_limit = f'{self.env_vars["memory"]}g' if self.env_vars.get(
            "memory") else c.CONTAINER_MEMORY_QUOTA
        container_stats = {}
        container_logs = None
        container = client.containers.run(
            f'getcarrier/{container_name}',
            command=self.command,
            mounts=mounts,
            stderr=True,
            remove=True,
            environment=self.env_vars,
            detach=True,
            nano_cpus=nano_cpus,
            mem_limit=mem_limit,
            **network_options
        )
        self.logger.info(f'Container obj: {container}')
        try:
            container_stats = container.stats(decode=False, stream=False)
            container_logs = container.logs(stream=True, follow=True)
        except Exception as e:
            self.logger.warning(f'Container stats are not available {e}')
            self.logger.warning(f'exc: {format_exc()}')
        if container_logs:
            logs = []
            for i in container_logs:
                line = i.decode('utf-8', errors='ignore')
                self.logger.info(f'{container_name} - {line}')
                logs.append(line)
            self.logger.info(f'Log stream ended for {container_name}')
            logs = ''.join(logs)
            match = re.search(r'memory used: (\d+ \w+).*?', logs, re.I)
            try:
                container_stats['memory_usage'] = match.group(1)
            except AttributeError:
                ...
        else:
            logs = "\n\n{logs are not available}"
        self.logger.info(f'Container stats: {container_stats}')
        self.logger.info(f'Container logs: {logs}')
        self.remove_volume(volume, attempts=ATTEMPTS_TO_REMOVE_VOL)
        return logs, container_stats

    def download_artifact(self, lambda_id: str) -> None:
        download_path = Path('/', 'tmp', lambda_id)
        download_path.mkdir()
        headers = {'Authorization': f'bearer {self.token}'}
        self.logger.info('Downloading artifact: %s', self.artifact_url)
        r = requests.get(self.artifact_url, allow_redirects=True, headers=headers, verify=c.SSL_VERIFY)
        self.logger.info('Artifact dl status: %s', r.status_code)
        with open(download_path.joinpath(lambda_id), 'wb') as file_data:
            file_data.write(r.content)

    @staticmethod
    def create_volume(client: DockerClient, lambda_id: str) -> Volume:
        volume = client.volumes.create(lambda_id)
        volume._centry_path = Path('/', 'tmp', volume.name)
        # LambdaExecutor.unzip_python(volume)
        LambdaExecutor.unzip_docker(volume)
        return volume

    @staticmethod
    def unzip_docker(volume: Volume) -> None:
        UNZIP_DOCKERFILE = """
FROM kubeless/unzip:latest
ADD {localfile} /tmp/{docker_path}
ENTRYPOINT ["unzip", "/tmp/{docker_path}", "-d", "/tmp/unzipped"]
        """

        UNZIP_DOCKER_COMPOSE = """
version: '3'
services:
  unzip:
    build: {path}
    volumes:
      - {volume}:/tmp/unzipped
    labels:
      - 'traefik.enable=false'
    container_name: unzip-{task_id}
volumes:
  {volume}:
    external: true
        """
        volume_path = volume._centry_path
        with open(volume_path.joinpath('Dockerfile'), 'w') as f:
            f.write(UNZIP_DOCKERFILE.format(
                localfile=volume.name,
                docker_path=f'{volume.name}.zip'
            ))
        with open(volume_path.joinpath('docker-compose.yaml'), 'w') as f:
            f.write(UNZIP_DOCKER_COMPOSE.format(
                path=volume_path,
                volume=volume.name,
                task_id=volume.name
            ))
        cmd = ['docker', 'compose', 'up']
        popen = Popen(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True,
                      cwd=volume_path)
        popen.communicate()
        cmd = ['docker', 'compose', 'down', '--rmi', 'all']
        popen = Popen(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True,
                      cwd=volume_path)
        popen.communicate()
        cmd = ['docker', 'builder', 'prune', '-f']
        popen = Popen(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True,
                      cwd=volume_path)
        popen.communicate()

    @staticmethod
    def unzip_local(volume: Volume) -> None:
        volume_path = volume._centry_path
        cmd = ['unzip', volume.name]
        popen = Popen(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True,
                      cwd=volume_path)
        popen.communicate()

    @staticmethod
    def unzip_python(volume: Volume) -> None:
        from zipfile import ZipFile
        with ZipFile(volume._centry_path.joinpath(volume.name), 'r') as zip_ref:
            zip_ref.extractall(volume._centry_path)
