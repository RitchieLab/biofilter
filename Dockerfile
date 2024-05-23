FROM mambaorg/micromamba
WORKDIR /app

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget tar unzip \
    && rm -rf /var/lib/apt/lists/*
USER $MAMBA_USER


RUN micromamba install -y -n base -c python=3.11 apsw sqlite  && \
    micromamba clean --all --yes
