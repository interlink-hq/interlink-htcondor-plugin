FROM htcondor/mini:23.0.25-el9

# Install EPEL and Apptainer for running Singularity container jobs,
# plus Flask and pyyaml for the plugin HTTP server.
# apptainer-suid provides the setuid binary so that HTCondor job processes
# (running as the unprivileged 'condor' user) can build SIF images from OCI
# sources inside a --privileged Docker container without needing user
# namespaces.  This avoids the "Could not write info to setgroups: Permission
# denied" error that occurs when only the non-setuid binary is installed.
# Version pinning is skipped because: epel-release has no meaningful version
# to pin on el9, and apptainer's latest stable is always what we want here.
# hadolint ignore=DL3041
RUN dnf install -y epel-release && \
    dnf install -y apptainer apptainer-suid && \
    dnf clean all && \
    rm -rf /var/cache/dnf/*

# Version pinning skipped: these are runtime dependencies of the plugin itself
# and are kept at their latest compatible releases to match the plugin source.
# hadolint ignore=DL3013,DL3042
RUN pip3 install --no-cache-dir flask pyyaml

# Expose apptainer as 'singularity' in /usr/bin (always on the default PATH
# that HTCondor provides to job processes), create output dirs, and configure
# HTCondor to keep /usr/bin on the job PATH so the singularity wrapper is
# always found at runtime.  Move condor_restd off port 8080 (to 9099) so that
# test containers can bind to port 8080 without conflict.
# Add /etc/subuid + /etc/subgid entries for the condor user so that newuidmap /
# newgidmap (used by Apptainer's user-namespace mode as a fallback) can map
# UIDs when running as non-root inside the container.
# hadolint ignore=DL3059
RUN ln -sf /usr/bin/apptainer /usr/bin/singularity && \
    sed -i 's/-p 8080/-p 9099/' /usr/local/bin/condor_restd.sh && \
    echo 'STARTER_JOB_ENVIRONMENT = "PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin APPTAINER_CACHEDIR=/tmp/apptainer-cache"' \
        > /etc/condor/config.d/99-job-env.conf && \
    mkdir -p /utils/.interlink /utils/out /utils/err /utils/log && \
    touch /utils/condor_config && \
    chmod 1777 /utils/.interlink /utils/out /utils/err /utils/log && \
    echo "condor:100000:65536" >> /etc/subuid && \
    echo "condor:100000:65536" >> /etc/subgid

WORKDIR /utils

# Pre-warm the Apptainer OCI blob cache for the images used by the vk-test-set
# integration tests.  Downloading and converting images here (as root, using
# the setuid binary) avoids the network fetch AND the user-namespace OCI->SIF
# conversion step at job runtime.  This step is placed before COPY so that the
# Docker layer cache remains valid across plugin code changes.
# Pulled SIF files are removed afterwards; only the OCI blob cache in
# APPTAINER_CACHEDIR is retained for use by HTCondor jobs.
# Cache directories are set to 1777 so the condor user can create new entries
# at job runtime (e.g. for images not pre-warmed here).
# hadolint ignore=DL3059
RUN mkdir -p /tmp/apptainer-cache /tmp/apptainer-prewarm && \
    chmod 1777 /tmp/apptainer-cache && \
    APPTAINER_CACHEDIR=/tmp/apptainer-cache APPTAINER_TMPDIR=/tmp/apptainer-prewarm \
    apptainer pull /tmp/apptainer-prewarm/cowsay.sif \
        docker://ghcr.io/grycap/cowsay || true && \
    APPTAINER_CACHEDIR=/tmp/apptainer-cache APPTAINER_TMPDIR=/tmp/apptainer-prewarm \
    apptainer pull /tmp/apptainer-prewarm/python-alpine.sif \
        docker://python:alpine || true && \
    APPTAINER_CACHEDIR=/tmp/apptainer-cache APPTAINER_TMPDIR=/tmp/apptainer-prewarm \
    apptainer pull /tmp/apptainer-prewarm/python311-alpine.sif \
        docker://python:3.11-alpine || true && \
    APPTAINER_CACHEDIR=/tmp/apptainer-cache APPTAINER_TMPDIR=/tmp/apptainer-prewarm \
    apptainer pull /tmp/apptainer-prewarm/alpine319.sif \
        docker://alpine:3.19 || true && \
    APPTAINER_CACHEDIR=/tmp/apptainer-cache APPTAINER_TMPDIR=/tmp/apptainer-prewarm \
    apptainer pull /tmp/apptainer-prewarm/busybox-latest.sif \
        docker://busybox:latest || true && \
    APPTAINER_CACHEDIR=/tmp/apptainer-cache APPTAINER_TMPDIR=/tmp/apptainer-prewarm \
    apptainer pull /tmp/apptainer-prewarm/busybox136.sif \
        docker://busybox:1.36 || true && \
    APPTAINER_CACHEDIR=/tmp/apptainer-cache APPTAINER_TMPDIR=/tmp/apptainer-prewarm \
    apptainer pull /tmp/apptainer-prewarm/nginx-alpine.sif \
        docker://nginx:alpine || true && \
    APPTAINER_CACHEDIR=/tmp/apptainer-cache APPTAINER_TMPDIR=/tmp/apptainer-prewarm \
    apptainer pull /tmp/apptainer-prewarm/bitnami-git.sif \
        docker://bitnamilegacy/git || true && \
    rm -rf /tmp/apptainer-prewarm && \
    find /tmp/apptainer-cache -type d -exec chmod 1777 {} + && \
    find /tmp/apptainer-cache -type f -exec chmod 644 {} +

# Copy plugin files (after cache-prewarm so Docker layer cache survives code changes)
COPY handles.py /utils/handles.py
COPY probes.py /utils/probes.py
COPY docker/SidecarConfig.yaml /utils/SidecarConfig.yaml

EXPOSE 8000

CMD ["/bin/sh", "-c", "/start.sh & python3 handles.py"]
