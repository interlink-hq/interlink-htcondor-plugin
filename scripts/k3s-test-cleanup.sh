#!/bin/bash
# k3s-test-cleanup.sh — Tear down the interLink + HTCondor e2e environment.
#
# Usage: bash scripts/k3s-test-cleanup.sh
#
# Safe to run multiple times; every step is best-effort (errors are ignored).

echo "=== Cleaning up interLink + HTCondor e2e environment ==="

# ---------------------------------------------------------------------------
# Locate test directory
# ---------------------------------------------------------------------------
TEST_DIR=""
if [[ -n "${TEST_DIR:-}" ]]; then
  echo "Using TEST_DIR from environment: ${TEST_DIR}"
elif [ -f /tmp/interlink-test-dir.txt ]; then
  TEST_DIR=$(cat /tmp/interlink-test-dir.txt)
  echo "Using TEST_DIR from state file: ${TEST_DIR}"
else
  echo "No test directory found; skipping process cleanup"
fi

# ---------------------------------------------------------------------------
# Stop Virtual Kubelet
# ---------------------------------------------------------------------------
if [ -n "${TEST_DIR}" ] && [ -f "${TEST_DIR}/vk.pid" ]; then
  VK_PID=$(cat "${TEST_DIR}/vk.pid")
  echo "Stopping Virtual Kubelet (PID ${VK_PID})..."
  kill "${VK_PID}" 2>/dev/null || true
  sleep 2
  kill -9 "${VK_PID}" 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# Stop interLink API
# ---------------------------------------------------------------------------
if [ -n "${TEST_DIR}" ] && [ -f "${TEST_DIR}/interlink-api.pid" ]; then
  API_PID=$(cat "${TEST_DIR}/interlink-api.pid")
  echo "Stopping interLink API (PID ${API_PID})..."
  kill "${API_PID}" 2>/dev/null || true
  sleep 2
  kill -9 "${API_PID}" 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# Stop background log-streaming processes
# ---------------------------------------------------------------------------
if [ -n "${TEST_DIR}" ]; then
  for pidfile in \
      "${TEST_DIR}/sidecar-log.pid" \
      "${TEST_DIR}/api-log.pid"; do
    if [ -f "${pidfile}" ]; then
      kill "$(cat "${pidfile}")" 2>/dev/null || true
    fi
  done
fi

# ---------------------------------------------------------------------------
# Persist Docker container logs and job data before stopping
# ---------------------------------------------------------------------------
if [ -n "${TEST_DIR}" ]; then
  echo "Saving container logs to ${TEST_DIR}..."
  docker logs htcondor-sidecar > "${TEST_DIR}/htcondor-sidecar.log" 2>&1 || true

  echo "Saving HTCondor system logs from container..."
  docker exec htcondor-sidecar \
    bash -c 'cat /var/log/condor/StarterLog 2>/dev/null || true' \
    > "${TEST_DIR}/condor-StarterLog.log" 2>&1 || true
  docker exec htcondor-sidecar \
    bash -c 'cat /var/log/condor/ShadowLog 2>/dev/null || true' \
    > "${TEST_DIR}/condor-ShadowLog.log" 2>&1 || true
  docker exec htcondor-sidecar \
    bash -c 'cat /var/log/condor/SchedLog 2>/dev/null || true' \
    > "${TEST_DIR}/condor-SchedLog.log" 2>&1 || true

  echo "Saving condor_history from container..."
  docker exec htcondor-sidecar \
    condor_history -long 2>/dev/null \
    > "${TEST_DIR}/condor-history.log" 2>&1 || true

  echo "Copying HTCondor job directories from container..."
  mkdir -p "${TEST_DIR}/plugin-jobs"
  docker cp htcondor-sidecar:/utils/.interlink/. \
    "${TEST_DIR}/plugin-jobs/" 2>&1 || true
fi

# ---------------------------------------------------------------------------
# Stop and remove Docker containers
# ---------------------------------------------------------------------------
echo "Removing Docker containers..."
docker stop htcondor-sidecar 2>/dev/null || true
docker rm   htcondor-sidecar 2>/dev/null || true

# ---------------------------------------------------------------------------
# Uninstall K3s
# ---------------------------------------------------------------------------
echo "Stopping K3s..."
if [ -f /usr/local/bin/k3s-uninstall.sh ]; then
  sudo /usr/local/bin/k3s-uninstall.sh 2>/dev/null || true
else
  echo "K3s uninstall script not found; skipping"
fi

# ---------------------------------------------------------------------------
# Optionally remove test directory
# ---------------------------------------------------------------------------
if [ -n "${TEST_DIR}" ]; then
  if [ "${REMOVE_TEST_DIR:-0}" = "1" ]; then
    echo "Removing test directory: ${TEST_DIR}"
    rm -rf "${TEST_DIR}" 2>/dev/null || true
    rm -f /tmp/interlink-test-dir.txt
  else
    echo "Preserving test directory for debugging: ${TEST_DIR}"
    echo "(set REMOVE_TEST_DIR=1 to remove it)"
  fi
fi

echo ""
echo "✓ Cleanup complete"
