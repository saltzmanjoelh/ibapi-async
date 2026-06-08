#!/usr/bin/env bash
# Start SSM port-forwarding tunnels to the IB Gateway running in the AWS ECS task.
#
# The gateway only accepts API connections from localhost. The SSM agent shares
# the task's network namespace, so forwarding through it lands on the same
# localhost the IB API and aux services are bound to.
#
# Forwards (local → remote, all on 127.0.0.1):
#   ${IBGATEWAY_PORT}    → 4004 (paper) or 4003 (live) — IB API
#   ${SCREENSHOT_PORT}   → 8080                        — screenshot HTTP server
#   ${NOVNC_PORT}        → 5900                        — noVNC web UI
#
# Usage:
#   bash scripts/start-gateway-tunnel.sh           # foreground (Ctrl-C to stop all)
#   nohup bash scripts/start-gateway-tunnel.sh \
#     > /tmp/tunnel.log 2>&1 &                     # background via shell
#
# Env:
#   AWS_PROFILE        named profile to use; unset/empty = ambient credentials
#                      (e.g. AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY exported by
#                      aws-actions/configure-aws-credentials in CI)
#   AWS_REGION         (default: us-east-1)
#   STACK_NAME         (default: ibkr-core)
#   ENVIRONMENT        (default: production)
#   IBGATEWAY_MODE     paper|live (default: paper)
#   IBGATEWAY_PORT     local API port        (default: 4002)
#   SCREENSHOT_PORT    local screenshot port (default: 8080)
#   NOVNC_PORT         local noVNC port      (default: 5900)

set -euo pipefail

PROFILE="${AWS_PROFILE-}"
REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="${STACK_NAME:-ibkr-core}"
ENVIRONMENT="${ENVIRONMENT:-production}"
MODE="${IBGATEWAY_MODE:-paper}"
LOCAL_API_PORT="${IBGATEWAY_PORT:-4002}"
LOCAL_SCREENSHOT_PORT="${SCREENSHOT_PORT:-8080}"
LOCAL_NOVNC_PORT="${NOVNC_PORT:-5900}"
CONTAINER_NAME="ibgateway"

for arg in "$@"; do
  case "$arg" in
    -h|--help)
      sed -n '2,32p' "$0"
      exit 0
      ;;
    *) echo "Unknown argument: $arg" >&2; exit 2 ;;
  esac
done

case "$MODE" in
  paper) REMOTE_API_PORT=4004 ;;
  live)  REMOTE_API_PORT=4003 ;;
  *) echo "IBGATEWAY_MODE must be 'paper' or 'live' (got: $MODE)" >&2; exit 2 ;;
esac

# When PROFILE is empty (CI / ambient credentials), omit --profile entirely so
# we don't accidentally select a non-existent named profile.
aws_call() {
  if [[ -n "$PROFILE" ]]; then
    aws --profile "$PROFILE" --region "$REGION" "$@"
  else
    aws --region "$REGION" "$@"
  fi
}

echo "→ Verifying AWS credentials (profile=${PROFILE:-<ambient>}, region=$REGION)..."
if ! aws_call sts get-caller-identity; then
  echo "AWS credentials check failed (profile=${PROFILE:-<ambient>})." >&2
  exit 1
fi

echo "→ Discovering ECS cluster/service from CloudFormation stack '$STACK_NAME'..."
CLUSTER_NAME="$(aws_call cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query 'Stacks[0].Outputs[?OutputKey==`ClusterName`].OutputValue' \
  --output text 2>/dev/null || true)"
SERVICE_NAME="$(aws_call cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query 'Stacks[0].Outputs[?OutputKey==`ServiceName`].OutputValue' \
  --output text 2>/dev/null || true)"

[[ -z "$CLUSTER_NAME" || "$CLUSTER_NAME" == "None" ]] && CLUSTER_NAME="ibgateway-cluster-${ENVIRONMENT}"
[[ -z "$SERVICE_NAME" || "$SERVICE_NAME" == "None" ]] && SERVICE_NAME="ibgateway-service-${ENVIRONMENT}"

echo "  Cluster: $CLUSTER_NAME"
echo "  Service: $SERVICE_NAME"

EXEC_ENABLED="$(aws_call ecs describe-services \
  --cluster "$CLUSTER_NAME" --services "$SERVICE_NAME" \
  --query 'services[0].enableExecuteCommand' --output text)"
if [[ "$EXEC_ENABLED" != "True" ]]; then
  echo "ECS service does not have enableExecuteCommand=True." >&2
  echo "Re-deploy the CloudFormation stack to enable it." >&2
  exit 1
fi

TASK_ARN="$(aws_call ecs list-tasks \
  --cluster "$CLUSTER_NAME" --service-name "$SERVICE_NAME" \
  --desired-status RUNNING \
  --query 'taskArns[0]' --output text)"
if [[ -z "$TASK_ARN" || "$TASK_ARN" == "None" ]]; then
  echo "No RUNNING tasks for service '$SERVICE_NAME'." >&2
  exit 1
fi
TASK_ID="${TASK_ARN##*/}"

RUNTIME_ID="$(aws_call ecs describe-tasks \
  --cluster "$CLUSTER_NAME" --tasks "$TASK_ARN" \
  --query "tasks[0].containers[?name=='${CONTAINER_NAME}'].runtimeId | [0]" \
  --output text)"
if [[ -z "$RUNTIME_ID" || "$RUNTIME_ID" == "None" ]]; then
  echo "Could not resolve runtimeId for container '$CONTAINER_NAME' in task $TASK_ID." >&2
  exit 1
fi

TARGET="ecs:${CLUSTER_NAME}_${TASK_ID}_${RUNTIME_ID}"

# (local_port, remote_port, label)
MAPS=(
  "${LOCAL_API_PORT} ${REMOTE_API_PORT} api-${MODE}"
  "${LOCAL_SCREENSHOT_PORT} 8080 screenshot"
  "${LOCAL_NOVNC_PORT} 5900 novnc"
)

PIDS=()
LOG_DIR="${TMPDIR:-/tmp}"
LOGS=()

cleanup() {
  trap - EXIT INT TERM
  echo
  echo "→ Stopping tunnels..."
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
  done
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

echo "→ Target: $TARGET"
echo

# Inline the ssm start-session invocation rather than going through aws_call so
# this script keeps working when launched via nohup (a child shell can't exec a
# function defined in the parent).
for entry in "${MAPS[@]}"; do
  read -r local_p remote_p label <<<"$entry"
  log="${LOG_DIR}/ibkr-tunnel-${label}.log"
  : > "$log"
  echo "→ ${label}: localhost:${local_p} → task:${remote_p} (log: ${log})"
  if [[ -n "$PROFILE" ]]; then
    aws --profile "$PROFILE" --region "$REGION" ssm start-session \
      --target "$TARGET" \
      --document-name AWS-StartPortForwardingSession \
      --parameters "{\"portNumber\":[\"${remote_p}\"],\"localPortNumber\":[\"${local_p}\"]}" \
      >"$log" 2>&1 < /dev/null &
  else
    aws --region "$REGION" ssm start-session \
      --target "$TARGET" \
      --document-name AWS-StartPortForwardingSession \
      --parameters "{\"portNumber\":[\"${remote_p}\"],\"localPortNumber\":[\"${local_p}\"]}" \
      >"$log" 2>&1 < /dev/null &
  fi
  PIDS+=($!)
  LOGS+=("$log")
done

# Wait for all listeners to come up before printing the summary.
echo
echo "→ Waiting for local listeners..."
for entry in "${MAPS[@]}"; do
  read -r local_p _ label <<<"$entry"
  for _ in $(seq 1 30); do
    if lsof -nP -iTCP:"$local_p" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "  ✓ ${label} ready on 127.0.0.1:${local_p}"
      break
    fi
    sleep 1
  done
done

echo
echo "================================================================"
echo " IB Gateway tunnels active. Open in your browser:"
echo "   Screenshot UI : http://127.0.0.1:${LOCAL_SCREENSHOT_PORT}/"
echo "   noVNC         : http://127.0.0.1:${LOCAL_NOVNC_PORT}/"
echo " API client connects to:"
echo "   127.0.0.1:${LOCAL_API_PORT}  (${MODE})"
echo " Logs: ${LOG_DIR}/ibkr-tunnel-*.log"
echo " Ctrl-C to stop all tunnels."
echo "================================================================"

# Block until any tunnel exits or the user hits Ctrl-C.
# (`wait -n` would be cleaner but isn't supported by bash 3.2 on macOS.)
while :; do
  for pid in "${PIDS[@]}"; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "Tunnel pid ${pid} exited; tearing down." >&2
      exit 1
    fi
  done
  sleep 2
done
