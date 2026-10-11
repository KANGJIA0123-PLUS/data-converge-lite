#!/usr/bin/env bash
# Kubernetes sends SIGTERM to the foreground Python process after this hook.
# Keep the persistent ledger and heartbeat for application-managed shutdown.
exit 0
