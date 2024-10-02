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
import json
from uuid import uuid4

import docker

from interceptor import constants as c
from interceptor.containers_backend import Client


class JobsWrapper(object):
    @staticmethod
    def dast(client: Client, container, execution_params, job_name, *args, **kwargs):
        #
        docker_container = container
        docker_name = f"dast_{uuid4()}"[:36]
        docker_command = execution_params["cmd"]
        docker_environment = {
            "project_id": execution_params["GALLOPER_PROJECT_ID"],
            "galloper_url": execution_params["GALLOPER_URL"],
            "token": execution_params["GALLOPER_AUTH_TOKEN"],
        }
        #
        if "SSL_CERTS" in execution_params:
            docker_environment["SSL_CERTS"] = execution_params["SSL_CERTS"]
        #
        docker_mounts = list()
        #
        return client.run(docker_container, name=docker_name, nano_cpus=c.CONTAINER_CPU_QUOTA,
                          mem_limit=c.CONTAINER_MEMORY_QUOTA, environment=docker_environment,
                          tty=True, detach=True, remove=c.REMOVE_CONTAINERS,
                          auto_remove=c.REMOVE_CONTAINERS, user="0:0", command=docker_command,
                          mounts=docker_mounts)

    @staticmethod
    def sast(client: Client, container, execution_params, job_name, *args, **kwargs):
        #
        docker_container = container
        docker_name = f"sast_{uuid4()}"[:36]
        docker_command = execution_params["cmd"]
        docker_environment = {
            "project_id": execution_params["GALLOPER_PROJECT_ID"],
            "galloper_url": execution_params["GALLOPER_URL"],
            "token": execution_params["GALLOPER_AUTH_TOKEN"],
        }
        #
        if "SSL_CERTS" in execution_params:
            docker_environment["SSL_CERTS"] = execution_params["SSL_CERTS"]
        #
        docker_mounts = list()
        #
        return client.run(docker_container, name=docker_name, nano_cpus=c.CONTAINER_CPU_QUOTA,
                          mem_limit=c.CONTAINER_MEMORY_QUOTA, environment=docker_environment,
                          tty=True, detach=True, remove=c.REMOVE_CONTAINERS,
                          auto_remove=c.REMOVE_CONTAINERS, user="0:0", command=docker_command,
                          mounts=docker_mounts)

    @staticmethod
    def dependency(client: Client, container, execution_params, job_name, *args, **kwargs):
        #
        docker_container = container
        docker_name = f"dependency_{uuid4()}"[:36]
        docker_command = execution_params["cmd"]
        docker_environment = {
            "project_id": execution_params["GALLOPER_PROJECT_ID"],
            "galloper_url": execution_params["GALLOPER_URL"],
            "token": execution_params["GALLOPER_AUTH_TOKEN"],
        }
        #
        if "SSL_CERTS" in execution_params:
            docker_environment["SSL_CERTS"] = execution_params["SSL_CERTS"]
        #
        docker_mounts = list()
        #
        return client.run(docker_container, name=docker_name, nano_cpus=c.CONTAINER_CPU_QUOTA,
                          mem_limit=c.CONTAINER_MEMORY_QUOTA, environment=docker_environment,
                          tty=True, detach=True, remove=c.REMOVE_CONTAINERS,
                          auto_remove=c.REMOVE_CONTAINERS, user="0:0", command=docker_command,
                          mounts=docker_mounts)

    @staticmethod
    def perfui(client: Client, container, execution_params, job_name, *args, **kwargs):
        return JobsWrapper.free_style(client, container, execution_params, job_name)

    @staticmethod
    def perfmeter(client: Client, container, execution_params, job_name, *args, **kwargs):
        env_vars = {"DISTRIBUTED_MODE_PREFIX": execution_params['DISTRIBUTED_MODE_PREFIX'],
                    "build_id": execution_params['build_id'],
                    "config_yaml": execution_params['config_yaml']}
        params = ['loki_host', 'loki_port', 'galloper_url', 'bucket', 'artifact',
                  'results_bucket', 'additional_files',
                  'JVM_ARGS', 'save_reports', 'project_id', 'token', "report_id", "cpu_quota",
                  "memory_quota", "custom_cmd", "integrations"]
        for key in params:
            if key in execution_params.keys():
                env_vars[key] = execution_params[key]
        if 'mounts' in execution_params.keys():
            mounts = json.loads(execution_params['mounts'])
            docker_mounts = []
            for key, value in mounts.items():
                docker_mounts.append(docker.types.Mount(target=value, source=key, type='bind'))
        else:
            docker_mounts = []
        name = f'{job_name}_{uuid4()}'[:36]

        nano_cpus = int(float(env_vars["cpu_quota"]) * c.CPU_MULTIPLIER) if env_vars.get(
            "cpu_quota") \
            else c.CONTAINER_CPU_QUOTA
        mem_limit = f'{env_vars["memory_quota"]}g' if env_vars.get(
            "memory_quota") else c.CONTAINER_MEMORY_QUOTA
        jvm_memory = int(mem_limit[:-1]) - 1
        env_vars["JVM_ARGS"] = f"-Xms{jvm_memory}g -Xmx{jvm_memory}g"
        logger = client.logger
        logger.info(f"Staring {container} with name {name}")
        logger.info(f"Command {execution_params['cmd']}")
        logger.info(f"env_vars: {env_vars}")
        return client.run(container, name=name, nano_cpus=nano_cpus, mem_limit=mem_limit,
                          environment=env_vars, tty=True, detach=True,
                          remove=c.REMOVE_CONTAINERS, auto_remove=c.REMOVE_CONTAINERS,
                          user='0:0', command=f"{execution_params['cmd']}",
                          mounts=docker_mounts)

    @staticmethod
    def free_style(client: Client, container, execution_params, job_name):
        if 'mounts' in execution_params.keys():
            mounts = json.loads(execution_params['mounts'])
            docker_mounts = []
            for key, value in mounts.items():
                docker_mounts.append(docker.types.Mount(target=value, source=key, type='bind'))
        else:
            docker_mounts = []
        command = execution_params.pop("cmd", "")
        return client.run(container, name=f'{job_name}_{uuid4()}'[:36],
                          nano_cpus=c.CONTAINER_CPU_QUOTA, mem_limit=c.CONTAINER_MEMORY_QUOTA,
                          environment=execution_params, tty=True, detach=True,
                          remove=c.REMOVE_CONTAINERS, auto_remove=c.REMOVE_CONTAINERS,
                          user='0:0', command=command, mounts=docker_mounts)

    @staticmethod
    def perfgun(client: Client, container, execution_params, job_name, *args, **kwargs):
        env_vars = {"DISTRIBUTED_MODE_PREFIX": execution_params['DISTRIBUTED_MODE_PREFIX'],
                    "GATLING_TEST_PARAMS": execution_params['GATLING_TEST_PARAMS'],
                    "build_id": execution_params['build_id'],
                    "config_yaml": execution_params['config_yaml']}
        params = ['influxdb_host', 'influxdb_port', 'influxdb_user', 'influxdb_password',
                  'influxdb_database',
                  'influxdb_comparison', "influxdb_telegraf", 'test_type', 'env', 'loki_host',
                  'loki_port',
                  'galloper_url', 'bucket', 'test', 'test_name', 'results_bucket', 'artifact',
                  'additional_files', 'save_reports',
                  'project_id', 'token', 'compile_and_run', 'JVM_ARGS', "report_id",
                  "cpu_quota", "memory_quota", "custom_cmd", "integrations"]
        for key in params:
            if key in execution_params.keys():
                env_vars[key] = execution_params[key]

        nano_cpus = int(float(env_vars["cpu_quota"]) * c.CPU_MULTIPLIER) if env_vars.get(
            "cpu_quota") \
            else c.CONTAINER_CPU_QUOTA
        mem_limit = f'{env_vars["memory_quota"]}g' if env_vars.get(
            "memory_quota") else c.CONTAINER_MEMORY_QUOTA
        jvm_memory = int(mem_limit[:-1]) - 1
        env_vars["JVM_ARGS"] = f"-Xms{jvm_memory}g -Xmx{jvm_memory}g"
        name = f'{job_name}_{uuid4()}'[:36]
        logger = client.logger
        logger.info(f"Staring {container} with name {name}")
        logger.info(f"Command {execution_params['cmd']}")
        logger.info(f"env_vars: {env_vars}")
        return client.run(container, name=name, nano_cpus=nano_cpus, mem_limit=mem_limit,
                          environment=env_vars, tty=True, detach=True,
                          remove=c.REMOVE_CONTAINERS, auto_remove=c.REMOVE_CONTAINERS,
                          user='0:0')

    @staticmethod
    def observer(client: Client, container, execution_params, job_name, *args, **kwargs):
        observer_container_name = f'{job_name}_{str(uuid4())[:8]}'
        env_vars = {}
        exclude_vars = ["cmd"]

        for k, v in execution_params.items():
            if k not in exclude_vars:
                env_vars[k] = v

        jira_params = execution_params.get("JIRA")

        if jira_params:
            jira_params = json.loads(jira_params)
            env_vars['JIRA_URL'] = jira_params["jira_url"]
            env_vars['JIRA_USER'] = jira_params["jira_login"]
            env_vars['JIRA_PASSWORD'] = jira_params["jira_password"]
            env_vars['JIRA_PROJECT'] = jira_params["jira_project"]

        ado_params = execution_params.get("ADO")
        if ado_params:
            ado_params = json.loads(ado_params)
            env_vars['ADO_ORGANIZATION'] = ado_params["ado_organization"]
            env_vars['ADO_PROJECT'] = ado_params["ado_project"]
            env_vars['ADO_TOKEN'] = ado_params["ado_token"]
            env_vars['ADO_TEAM'] = ado_params["ado_team"]

        docker_mounts = []

        if 'mounts' in execution_params.keys():
            for mount in execution_params['mounts']:
                for key, value in mount.items():
                    docker_mounts.append(
                        docker.types.Mount(target=value, source=key, type='bind'))

        observer_command = execution_params['cmd']

        return client.run(container, name=observer_container_name,
                          nano_cpus=c.CONTAINER_CPU_QUOTA, mem_limit=c.CONTAINER_MEMORY_QUOTA,
                          environment=env_vars, tty=True, detach=True,
                          remove=c.REMOVE_CONTAINERS, auto_remove=c.REMOVE_CONTAINERS,
                          user='0:0', command=observer_command, mounts=docker_mounts)

    @staticmethod
    def browsertime(client: Client, container, env_vars, cmd, *args, **kwargs):
        return client.run(container, name=f'browsertime_{uuid4()}'[:36],
                          nano_cpus=c.BROWSERTIME_CPU_QUOTA,
                          mem_limit=c.BROWSERTIME_MEMORY_QUOTA, environment=env_vars, tty=True,
                          detach=True, remove=c.REMOVE_CONTAINERS,
                          auto_remove=c.REMOVE_CONTAINERS, user='0:0', command=cmd)
