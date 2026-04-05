FROM htcondor/mini:23.0.25-el9

# Install EPEL and Apptainer for running Singularity container jobs,
# plus Flask and pyyaml for the plugin HTTP server.
RUN dnf install -y epel-release && \
    dnf install -y apptainer && \
    dnf clean all && \
    rm -rf /var/cache/dnf/*

RUN pip3 install flask pyyaml

# Expose apptainer as 'singularity' so existing plugin scripts resolve it.
RUN ln -sf /usr/bin/apptainer /usr/local/bin/singularity

COPY handles.py /utils/handles.py
COPY probes.py /utils/probes.py
COPY docker/SidecarConfig.yaml /utils/SidecarConfig.yaml
RUN touch /utils/condor_config

# Create HTCondor output directories and plugin data directories.
RUN mkdir -p /utils/.interlink /utils/out /utils/err /utils/log

WORKDIR /utils

EXPOSE 8000

CMD ["/bin/sh", "-c", "/start.sh & python3 handles.py"]
