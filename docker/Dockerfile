FROM htcondor/mini:23.0.25-el9

# Install EPEL and Apptainer for running Singularity container jobs,
# plus Flask and pyyaml for the plugin HTTP server.
# Version pinning is skipped because: epel-release has no meaningful version
# to pin on el9, and apptainer's latest stable is always what we want here.
# hadolint ignore=DL3041
RUN dnf install -y epel-release && \
    dnf install -y apptainer && \
    dnf clean all && \
    rm -rf /var/cache/dnf/*

# Version pinning skipped: these are runtime dependencies of the plugin itself
# and are kept at their latest compatible releases to match the plugin source.
# hadolint ignore=DL3013,DL3042
RUN pip3 install --no-cache-dir flask pyyaml

# Expose apptainer as 'singularity' in /usr/bin (always on the default PATH
# that HTCondor provides to job processes), create output dirs, and copy plugin
# files.  Also configure HTCondor to keep /usr/bin on the job PATH so the
# singularity wrapper is always found at runtime.
COPY handles.py /utils/handles.py
COPY probes.py /utils/probes.py
COPY docker/SidecarConfig.yaml /utils/SidecarConfig.yaml
# hadolint ignore=DL3059
RUN ln -sf /usr/bin/apptainer /usr/bin/singularity && \
    echo 'STARTER_JOB_ENVIRONMENT = "PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin APPTAINER_CACHEDIR=/tmp/apptainer-cache"' \
        > /etc/condor/config.d/99-job-env.conf && \
    touch /utils/condor_config && \
    mkdir -p /utils/.interlink /utils/out /utils/err /utils/log

WORKDIR /utils

EXPOSE 8000

CMD ["/bin/sh", "-c", "/start.sh & python3 handles.py"]
