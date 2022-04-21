# Misty

Code repository for paper "Misty: Microservice-based Streaming Trajectory Similarity Search"

## Quick start

```shell
conda env create -f env.yml
conda activate misty
python tests/test.py
```

## Dataset

We use a selected subset of T-Drive dataset presented in `data/filtered` folder. The preprocessing code can be found in
`preprocess.ipynb`

## Module list

![framework](./framework.png)

| Module name in framework | Folder name                  |
|--------------------------|------------------------------|
| Point Stream | [ingress](./ingress/)         |
| Assembler | [assembler](./assemble/)     |
| Index | [index](./index/)            |
| Coordinator | [coordinator](./index_meta/) |
| Executor | [executor](./compute/)       |
| Qyery Agent | [agent](./agent/)            |
