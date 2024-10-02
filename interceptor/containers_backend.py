import logging
from abc import ABC, abstractmethod
from uuid import uuid4
from datetime import datetime

import docker
import requests
import urllib3
from docker.errors import NotFound
from kubernetes import client
from kubernetes.client import ApiClient, V1EnvVar, ApiException, V1SecurityContext, \
    V1Volume

from interceptor import constants as c
from interceptor.constants import LAMBDA_CONTAINER_REPO
from interceptor.utils import build_api_url

NANO_TO_MILL_MULTIPLIER = 1000000


class Client(ABC):

    @abstractmethod
    def __init__(self, logger: logging.Logger, *args, **kwargs):
        self.logger = logger

    @abstractmethod
    def run(self, image: str, name, nano_cpus, mem_limit, environment, tty, detach, remove,
            auto_remove, user, command=None, mounts=None
    ):
        raise NotImplementedError

    @abstractmethod
    def info(self):
        raise NotImplementedError


class Job(ABC):

    @abstractmethod
    def is_finished(self):
        raise NotImplementedError

    @abstractmethod
    def stop_job(self):
        raise NotImplementedError

    @abstractmethod
    def log_status(self, last_logs: list):
        raise NotImplementedError

    @abstractmethod
    def send_resource_usage(self, job_type, params, time_to_sleep=None):
        raise NotImplementedError



class DockerJob(Job):

    def __init__(self, cid, logger: logging.Logger):
        self.logger = logger
        self.cid = cid
        self.client_lowlevel = docker.APIClient(base_url='unix://var/run/docker.sock')

    def is_finished(self):
        return self.cid.status == "exited"

    def stop_job(self):
        self.cid.stop(timeout=5)

    @property
    def container_stats(self) -> str:
        template = f'Container {self.cid.id}\n\tSTATUS: {self.cid.status}'
        resource_usage = self.client_lowlevel.stats(self.cid.id, stream=False)
        try:
            cpu = round(float(resource_usage["cpu_stats"]["cpu_usage"]["total_usage"]) / c.CPU_MULTIPLIER, 2)
            template += f'\n\tCPU: {cpu}'
        except KeyError:
            self.logger.warning('Cannot get cpu stats')
            self.logger.debug(f'resource_usage: {resource_usage}')
        try:
            ram = round(float(resource_usage["memory_stats"]["usage"]) / (1024 * 1024), 2)
            ram_limit = round(float(resource_usage["memory_stats"]["limit"]) / (1024 * 1024), 2)
            template += f'\n\tRAM: {ram} / {ram_limit} Mb'
        except KeyError:
            self.logger.warning('Cannot get ram stats')
            self.logger.debug(f'resource_usage: {resource_usage}')
        return template

    def log_status(self, last_logs: list) -> None:
        try:
            self.cid.reload()
            self.logger.info(self.container_stats)
            logs = self.client_lowlevel.logs(
                self.cid.id, stream=False, tail=100).decode(
                "utf-8",
                errors='ignore').split(
                '\r\n')
            for each in logs:
                if each not in last_logs:
                    self.logger.info(each)
                    last_logs.append(each)
        except NotFound:
            self.logger.info('Container terminated')
            self.cid.attrs["State"] = "exited"

    def send_resource_usage(self, job_type, params, time_to_sleep=None):
        base_url = params.get("galloper_url") or params.get("GALLOPER_URL")
        token = params.get("token")
        statisticts_url = build_api_url('usage', 'vcu', mode='default')
        url = f"{base_url}{statisticts_url}/{params['project_id']}"
        resource_usage = self.client_lowlevel.stats(self.cid.id, stream=False)
        data = {
            'report_id': params['report_id'],
            'job_type': job_type,
            'time': str(datetime.now()),
            'time_to_sleep': time_to_sleep,
            'cpu': round(float(resource_usage["cpu_stats"]["cpu_usage"]["total_usage"]) / c.CPU_MULTIPLIER, 2),
            'memory_usage': round(float(resource_usage["memory_stats"]["usage"]) / (1024 * 1024), 2),
            'memory_limit': round(float(resource_usage["memory_stats"]["limit"]) / (1024 * 1024), 2),
        }
        headers = {'content-type': 'application/json'}
        if token:
            headers['Authorization'] = f'bearer {token}'
        requests.put(url, json=data, headers=headers, verify=c.SSL_VERIFY)

    @property
    def status(self):
        return self.cid.status

    @property
    def id(self):
        return self.cid.id

    def reload(self):
        return self.cid.reload()


class KubernetesJob(Job):

    def __init__(
            self, api_client: ApiClient, job_name: str, logger: logging.Logger, namespace: str
    ):
        self.namespace = namespace
        self.logger = logger
        self.job_name = job_name
        self.api_client = api_client
        self.batch_v1 = client.BatchV1Api(api_client)
        self.core_api = client.CoreV1Api(api_client)

    def is_finished(self):
        try:
            api_response = self.batch_v1.read_namespaced_job_status(
                name=self.job_name,
                namespace=self.namespace
            )
        except ApiException:
            self.logger.error(f"Error while checking job status")
            return True
        else:
            if api_response.status.succeeded == api_response.spec.completions:
                return True
            if api_response.status.failed is not None:
                self.logger.error(f"Job has been failed {api_response.status}")
                return True
        return False

    def stop_job(self):
        self.batch_v1.delete_namespaced_job(self.job_name, namespace=self.namespace,
                                            grace_period_seconds=15,
                                            propagation_policy="Foreground")

    def log_status(self, last_logs: list):
        try:
            pods = self.core_api.list_namespaced_pod(
                namespace=self.namespace, label_selector=f"job-name={self.job_name}")
        except ApiException as exc:
            self.logger.warning(exc)
        else:
            for idx, pod in enumerate(pods.items):
                pod_logs = self.core_api.read_namespaced_pod_log(
                    pod.metadata.name, namespace=self.namespace).split("\n")
                for log in pod_logs:
                    if log not in last_logs:
                        self.logger.info(f"[runner {idx + 1}] {log}")
                        last_logs.append(log)

    def send_resource_usage(self, job_type, params, time_to_sleep=None):
        base_url = params.get("galloper_url") or params.get("GALLOPER_URL")
        token = params.get("token")
        statisticts_url = build_api_url('usage', 'vcu', mode='default')
        url = f"{base_url}{statisticts_url}/{params['project_id']}"
        resource_usage = self.collect_resource_usage()
        data = {
            'time': str(datetime.now()),
            'report_id': params['report_id'],
            'time_to_sleep': time_to_sleep,
            'job_type': job_type,
            'stats': resource_usage
        }
        headers = {'content-type': 'application/json'}
        if token:
            headers['Authorization'] = f'bearer {token}'
        requests.put(url, json=data, headers=headers, verify=c.SSL_VERIFY)

    def collect_resource_usage(self):
        resource_usage = []
        try:
            pods = self.core_api.list_namespaced_pod(
                namespace=self.namespace, label_selector=f"job-name={self.job_name}")
        except ApiException as exc:
            self.logger.warning(exc)
        else:
            for idx, pod in enumerate(pods.items):
                container = pod.spec.containers[0]
                container_limits = container.resources.limits
                resource_usage.append({
                    'pod': pod.metadata.name,
                    'cpu_limit': container_limits['cpu'],
                    'memory_limit': container_limits['memory']})
        return resource_usage

class DockerClient(Client):

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.docker = docker.from_env()

    def info(self):
        return self.docker.info()

    def run(self, image: str, **kwargs):
        container = self.docker.containers.run(image, **kwargs)
        return DockerJob(cid=container, logger=self.logger)

    @property
    def volumes(self):
        return self.docker.volumes


class KubernetesClient(Client):

    def __init__(
            self, logger, token, host, jobs_count: int = 1,
            secure_connection: bool = False,
            namespace: str = "default",
            scaling_cluster: bool = False,
            mode: str = 'default', **kwargs
    ):
        self.namespace = namespace
        self.jobs_count = jobs_count
        self.token = token
        self.host = host
        self.secure_connection = secure_connection
        self.logger = logger
        self.scaling_cluster = scaling_cluster
        self.mode = mode
        self.api_version = kwargs.get('api_version', 1)

        self.api_client = self._prepare_api_client()
        self.batch_v1 = client.BatchV1Api(self.api_client)
        self.core_api = client.CoreV1Api(self.api_client)
        self.JOB_NAME = f"test-{str(uuid4())}"

    def _prepare_api_client(self) -> ApiClient:
        configuration = client.Configuration()
        configuration.api_key_prefix['authorization'] = 'Bearer'
        configuration.api_key['authorization'] = self.token
        configuration.host = self.host
        configuration.verify_ssl = self.secure_connection

        if not self.secure_connection:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        return ApiClient(configuration)

    def get_capacity(self, url: str, bearer_token: str):
        kuber_url = build_api_url('kubernetes', 'get_available_resources', mode=self.mode,
                                  api_version=self.api_version)
        url = f"{url}{kuber_url}"
        data = {
            "hostname": self.host,
            "k8s_token": {"value": self.token, "from_secrets": False},
            "namespace": self.namespace,
            "secure_connection": self.secure_connection
        }
        headers = {'content-type': 'application/json',
                   'Authorization': f'bearer {bearer_token}'}
        res = requests.post(url, json=data, headers=headers, verify=c.SSL_VERIFY)
        res.raise_for_status()
        capacity = res.json()
        return capacity

    def create_job(
            self, image, name: str,
            env_vars: dict, command: str,
            nano_cpus, mem_limit
    ):
        container = client.V1Container(
            name=name.replace("_", "-"),
            image=image,
            resources=client.V1ResourceRequirements(
                limits={"cpu": f"{nano_cpus / NANO_TO_MILL_MULTIPLIER}m",
                        "memory": f"{mem_limit}".upper()},
                requests={"cpu": f"{nano_cpus / NANO_TO_MILL_MULTIPLIER}m",
                          "memory": f"{mem_limit}".upper()},
            ),
            env=[V1EnvVar(key, str(value)) for key, value in env_vars.items()],
            args=command.split(),
            security_context=V1SecurityContext(run_as_user=0, run_as_group=0)
        )

        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=self.JOB_NAME),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(labels={"app": "interceptor"}),
                    spec=client.V1PodSpec(containers=[container], restart_policy="Never"),
                ),
                completions=self.jobs_count,
                parallelism=self.jobs_count,
                ttl_seconds_after_finished=10
            )
        )
        api_response = self.batch_v1.create_namespaced_job(
            body=job,
            namespace=self.namespace,
        )
        return api_response

    def info(self):
        pass

    def run(self, image: str, name, nano_cpus, mem_limit, environment, tty=None,
            detach=None, remove=None,
            auto_remove=None, user=None, command="", mounts=None
    ) -> KubernetesJob:

        if not self.scaling_cluster:
            base_url = environment.get("galloper_url") or environment.get("GALLOPER_URL")
            capacity = self.get_capacity(base_url, environment["token"])
            if self.jobs_count > capacity["pods"]:
                raise ValueError("Not enough runners")
            required_cpu = (nano_cpus / (NANO_TO_MILL_MULTIPLIER * 1000)) * self.jobs_count
            required_memory = int(mem_limit[:-1]) * self.jobs_count
            if required_cpu > capacity["cpu"] or required_memory > capacity["memory"]:
                raise ValueError("Not enough capacity in cluster to run test")

        self.create_job(image, name, environment, command=command,
                        nano_cpus=nano_cpus, mem_limit=mem_limit)
        return KubernetesJob(self.api_client, self.JOB_NAME, self.logger, self.namespace)

    def create_lambda_job(self, image, auth_token, environment, artifact_url, command):
        shared_volume_mount = client.V1VolumeMount(
            name="shared-data",
            mount_path="/tmp",
        )

        download_task = client.V1Container(
            name="wget",
            image="busybox:latest",
            command=["/bin/sh", "-c"],
            args=[
                f"wget '{artifact_url}' "
                f"--header='Authorization: bearer {auth_token}' -O tmp/task.zip"],
            resources=client.V1ResourceRequirements(
                limits={"cpu": "250m", "memory": "250Mi"},
                requests={"cpu": "250m", "memory": "250Mi"},
            ),
            volume_mounts=[shared_volume_mount]
        )

        unzip_task = client.V1Container(
            name="unzip",
            image="busybox:latest",
            command=["/bin/sh", "-c"],
            args=["unzip tmp/task.zip -d tmp/"],
            resources=client.V1ResourceRequirements(
                limits={"cpu": "250m", "memory": "250Mi"},
                requests={"cpu": "250m", "memory": "250Mi"},
            ),
            volume_mounts=[shared_volume_mount]
        )

        task = client.V1Container(
            name="main",
            image=f"{LAMBDA_CONTAINER_REPO}/{image}",
            args=command,
            resources=client.V1ResourceRequirements(
                limits={"cpu": "1000m", "memory": "1G"},
                requests={"cpu": "1000m", "memory": "1G"},
            ),
            volume_mounts=[client.V1VolumeMount(
                name="shared-data",
                mount_path="/var/task"
            )],
            env=[V1EnvVar(key, str(value)) for key, value in environment.items()]
        )

        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=self.JOB_NAME),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(labels={"app": "interceptor"}),
                    spec=client.V1PodSpec(
                        containers=[task],
                        init_containers=[download_task, unzip_task],
                        restart_policy="Never",
                        automount_service_account_token=False,
                        volumes=[
                            V1Volume(
                                name="shared-data",
                                empty_dir=client.V1EmptyDirVolumeSource()
                            )]
                    ),
                ),
                backoff_limit=3,
                ttl_seconds_after_finished=30
            ))

        api_response = self.batch_v1.create_namespaced_job(
            body=job,
            namespace=self.namespace,
        )
        return api_response

    def run_lambda(self, image, auth_token, environment, artifact_url, command):
        self.JOB_NAME = f"lambda-job-{uuid4()}"
        self.create_lambda_job(image, auth_token, environment, artifact_url, command)
        return KubernetesJob(self.api_client, self.JOB_NAME, self.logger, self.namespace)
