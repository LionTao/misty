FROM mambaorg/micromamba:0.22.0

USER root
RUN sed -i "s/deb.debian.org/mirror.sjtu.edu.cn/g" /etc/apt/sources.list &&\
    sed -i "s/security.debian.org/mirror.sjtu.edu.cn/g" /etc/apt/sources.list &&\
    apt-get update &&\
    apt-get -y upgrade &&\
    apt-get install --no-install-recommends -y patchelf curl ca-certificates build-essential ccache &&\
    apt-get clean &&\
    rm -rf /var/lib/apt/lists/*

USER $MAMBA_USER
ARG MAMBA_DOCKERFILE_ACTIVATE=1

COPY --chown=$MAMBA_USER:$MAMBA_USER env.yaml /tmp/env.yaml
ENV PIP_INDEX_URL https://mirror.sjtu.edu.cn/pypi/web/simple
RUN micromamba install -y -c https://mirror.sjtu.edu.cn/anaconda/cloud/conda-forge -f /tmp/env.yaml && \
    micromamba clean --all --yes

COPY --chown=micromamba:micromamba . /app/
WORKDIR /app/