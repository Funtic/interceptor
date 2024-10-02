import pytest
import requests_mock
import docker
import tests.utils.constants as c
from interceptor.jobs_wrapper import JobsWrapper

galloper_url = "https://example.com"
project_id = 1
token = "test"


def test_job_wrapper_dast():
    with requests_mock.Mocker() as mock:
        client = docker.from_env()
        mock.register_uri(requests_mock.POST, c.docker_create_url, status_code=204, json={"Id": "1"})
        mock.get(c.docker_get_container_info_url, json={"Id": "1"})
        mock.post(c.docker_start_url, json={"Id": "1"})
        JobsWrapper.dast(client, "getcarrier/dast:beta-2.0", {"cmd": "", "GALLOPER_PROJECT_ID": project_id,
                                                            "GALLOPER_URL": galloper_url,
                                                            "GALLOPER_AUTH_TOKEN": token}, "dast")
        assert mock.call_count == 3


def test_job_wrapper_sast():
    with requests_mock.Mocker() as mock:
        client = docker.from_env()
        mock.register_uri(requests_mock.POST, c.docker_create_url, status_code=204, json={"Id": "1"})
        mock.get(c.docker_get_container_info_url, json={"Id": "1"})
        mock.post(c.docker_start_url, json={"Id": "1"})
        JobsWrapper.sast(client, "getcarrier/sast:beta-2.0", {"cmd": "", "GALLOPER_PROJECT_ID": project_id,
                                                            "GALLOPER_URL": galloper_url,
                                                            "GALLOPER_AUTH_TOKEN": token}, "sast")
        assert mock.call_count == 3


def test_job_wrapper_dependency():
    with requests_mock.Mocker() as mock:
        client = docker.from_env()
        mock.register_uri(requests_mock.POST, c.docker_create_url, status_code=204, json={"Id": "1"})
        mock.get(c.docker_get_container_info_url, json={"Id": "1"})
        mock.post(c.docker_start_url, json={"Id": "1"})
        JobsWrapper.dependency(client, "getcarrier/dependency:beta-2.0", {"cmd": "", "GALLOPER_PROJECT_ID": project_id,
                                                            "GALLOPER_URL": galloper_url,
                                                            "GALLOPER_AUTH_TOKEN": token}, "dependency")
        assert mock.call_count == 3


def test_job_wrapper_perfmeter():
    with requests_mock.Mocker() as mock:
        client = docker.from_env()
        mock.register_uri(requests_mock.POST, c.docker_create_url, status_code=204, json={"Id": "1"})
        mock.get(c.docker_get_container_info_url, json={"Id": "1"})
        mock.post(c.docker_start_url, json={"Id": "1"})
        JobsWrapper.perfmeter(client, "getcarrier/perfmeter:beta-2.0", {"cmd": "", "DISTRIBUTED_MODE_PREFIX": "prefix",
                                                                      "build_id": "test_build",
                                                                      "config_yaml": {}}, "perfmeter")
        assert mock.call_count == 3


def test_job_wrapper_perfgun():
    with requests_mock.Mocker() as mock:
        client = docker.from_env()
        mock.register_uri(requests_mock.POST, c.docker_create_url, status_code=204, json={"Id": "1"})
        mock.get(c.docker_get_container_info_url, json={"Id": "1"})
        mock.post(c.docker_start_url, json={"Id": "1"})
        JobsWrapper.perfgun(client, "getcarrier/perfgun:beta-2.0", {"cmd": "", "DISTRIBUTED_MODE_PREFIX": "prefix",
                                                                  "build_id": "test_build", "GATLING_TEST_PARAMS": "",
                                                                  "config_yaml": {}}, "perfgun")
        assert mock.call_count == 3


def test_job_wrapper_observer():
    with requests_mock.Mocker() as mock:
        client = docker.from_env()
        mock.register_uri(requests_mock.POST, c.docker_create_url, status_code=204, json={"Id": "1"})
        mock.get(c.docker_get_container_info_url, json={"Id": "1"})
        mock.post(c.docker_start_url, json={"Id": "1"})
        JobsWrapper.observer(client, "getcarrier/observer:beta-2.0", {"cmd": ""}, "observer")
        assert mock.call_count == 3


def test_job_wrapper_browsertime():
    with requests_mock.Mocker() as mock:
        client = docker.from_env()
        mock.register_uri(requests_mock.POST, c.docker_create_url, status_code=204, json={"Id": "1"})
        mock.get(c.docker_get_container_info_url, json={"Id": "1"})
        mock.post(c.docker_start_url, json={"Id": "1"})
        JobsWrapper.observer(client, "getcarrier/browsertime:beta-2.0", {"cmd": ""}, "browsertime")
        assert mock.call_count == 3


def test_job_wrapper_perfui():
    with requests_mock.Mocker() as mock:
        client = docker.from_env()
        mock.register_uri(requests_mock.POST, c.docker_create_url, status_code=204, json={"Id": "1"})
        mock.get(c.docker_get_container_info_url, json={"Id": "1"})
        mock.post(c.docker_start_url, json={"Id": "1"})
        JobsWrapper.perfui(client, "getcarrier/perfui:beta-2.0", {"cmd": ""}, "perfui")
        assert mock.call_count == 3
