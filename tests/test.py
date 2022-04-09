import json
import os
import shutil
import subprocess
import sys
from copy import deepcopy
from time import sleep
from typing import List

from tqdm import tqdm

sys.path.append(os.curdir)


def file_name_creator():
    trajectories = list(range(1, 151))
    template = {
        "result_file_name": "test",
        "trajectories": trajectories,
        "INIT_RESOLUTION": 1,
        "MAX_BUFFER_SIZE": 50,
        "TREE_INSERTION_THRESHOLD": 0.2,
        "SPLIT_THRESHOLD": 2000
    }
    settings = []
    for m in [0, 1, 10, 100, 300, 500, 700]:
        temp = deepcopy(template)
        temp["MAX_BUFFER_SIZE"] = m
        temp[
            "result_file_name"] = f'{temp["MAX_BUFFER_SIZE"]}_{temp["TREE_INSERTION_THRESHOLD"]}_{temp["SPLIT_THRESHOLD"]}'
        settings.append(temp)
    for k in [1000, 1500, 2000, 2500, 3000]:
        temp = deepcopy(template)
        temp["TREE_INSERTION_THRESHOLD"] = k
        temp[
            "result_file_name"] = f'{temp["MAX_BUFFER_SIZE"]}_{temp["TREE_INSERTION_THRESHOLD"]}_{temp["SPLIT_THRESHOLD"]}'
        settings.append(temp)
        temp = deepcopy(temp)
        temp["MAX_BUFFER_SIZE"] = 0
        temp[
            "result_file_name"] = f'{temp["MAX_BUFFER_SIZE"]}_{temp["TREE_INSERTION_THRESHOLD"]}_{temp["SPLIT_THRESHOLD"]}'
        settings.append(temp)
    for t in [0, 0.1, 0.3, 0.5, 0.9]:
        temp = deepcopy(template)
        temp["SPLIT_THRESHOLD"] = t
        temp[
            "result_file_name"] = f'{temp["MAX_BUFFER_SIZE"]}_{temp["TREE_INSERTION_THRESHOLD"]}_{temp["SPLIT_THRESHOLD"]}'
        settings.append(temp)
        temp = deepcopy(temp)
        temp["MAX_BUFFER_SIZE"] = 0
        temp[
            "result_file_name"] = f'{temp["MAX_BUFFER_SIZE"]}_{temp["TREE_INSERTION_THRESHOLD"]}_{temp["SPLIT_THRESHOLD"]}'
        settings.append(temp)

    # plain rtree
    temp = deepcopy(template)
    template["SPLIT_THRESHOLD"] = float('inf')
    template["TREE_INSERTION_THRESHOLD"] = 0
    template["MAX_BUFFER_SIZE"] = 0
    template[
        "result_file_name"] = f'{template["MAX_BUFFER_SIZE"]}_{template["TREE_INSERTION_THRESHOLD"]}_{template["SPLIT_THRESHOLD"]}'
    settings.append(temp)

    return settings


def start_assembler(num_workers: int):
    assert num_workers <= 100
    dapr = shutil.which("dapr")
    res = []
    for i in range(num_workers):
        p = 3000 + i
        pid = subprocess.Popen(
            f"{dapr} run --app-id assemble --app-port {p} -- hypercorn   --bind 0.0.0.0:{p} assemble.main:app",
            shell=True)
        res.append(pid)
    return res


def start_index(num_workers: int):
    assert num_workers <= 100
    dapr = shutil.which("dapr")
    res = []
    for i in range(num_workers):
        p = 3100 + i
        pid = subprocess.Popen(
            f"{dapr} run --app-id index --app-port {p} -- hypercorn   --bind 0.0.0.0:{p} index.main:app", shell=True)
        res.append(pid)
    return res


def start_index_meta():
    p = 3300
    dapr = shutil.which("dapr")
    return [subprocess.Popen(
        f"{dapr} run --app-id index-meta --app-port {p} -- hypercorn   --bind 0.0.0.0:{p} index_meta.main:app",
        shell=True)]


def start_compute(num_workers: int):
    assert num_workers <= 100
    dapr = shutil.which("dapr")
    res = []
    for i in range(num_workers):
        p = 3200 + i
        pid = subprocess.Popen(
            f"{dapr} run --app-id compute --app-port {p} -- hypercorn  --bind 0.0.0.0:{p}  compute.main:app",
            shell=True)
        res.append(pid)
    return res


def start_agent():
    p = 3301
    dapr = shutil.which("dapr")
    return [subprocess.Popen(
        f"{dapr} run --app-id agent --app-port {p} -- hypercorn   --bind 0.0.0.0:{p} agent.main:app", shell=True)]


def start():
    os.system("dapr init --from-dir /home/liontao/Downloads/daprbundle")
    # client = docker.DockerClient(version='1.21')
    os.system("docker run -d --name jaeger \
              -e COLLECTOR_ZIPKIN_HOST_PORT=:9412 \
              -p 16686:16686 \
              -p 9412:9412 \
              jaegertracing/all-in-one:1.22")
    os.system(
        'sed -i "s@http://localhost:9411/api/v2/spans@http://localhost:9412/api/v2/spans@g" /home/liontao/.dapr/config.yaml')
    os.system('docker run --name "dapr_zipkin" --restart always -d -p 9411:9411 openzipkin/zipkin')
    os.system('docker run --name "dapr_redis" --restart always -d -p 6379:6379 redislabs/rejson')
    assemblers = start_assembler(2)
    indexes = start_index(1)
    meta = start_index_meta()
    computes = start_compute(1)
    agent = start_agent()
    return [*assemblers, *indexes, meta, *computes, agent]


def stop(p_list: List[subprocess.Popen[str]]):
    for p in p_list:
        p.terminate()
    os.system("/bin/sh ./stop.sh")


def experiment():
    for setting in tqdm(file_name_creator()):
        if os.path.exists(f"tests/results/{setting['result_file_name']}.json"):
            continue
        with open("tests/parameters.json", 'w') as f:
            json.dump(setting, f)
        os.system('/bin/bash ./start.sh')
        sleep(2)
        os.system('/bin/bash ./stop.sh')
    # p = start()
    # with open("tests/parameters.json") as f:
    #     para: dict = json.load(f)
    # res_fname: str = para["result_file_name"]
    # target_trajectories: List[int] = para["trajectories"]
    # run_test(target_trajectories, res_fname)
    # stop(p)


if __name__ == '__main__':
    experiment()
