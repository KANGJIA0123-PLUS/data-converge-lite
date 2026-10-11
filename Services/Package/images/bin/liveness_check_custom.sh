#!/usr/bin/env bash
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

set -e
source "./function.sh"

KEY_STORE_CERT_FILE="/opt/its/data-converge/secret/server.p12"
KEY_STORE_CERT_HASH_FILE="/tmp/server.db"
TRUST_STORE_CERT_FILE="/opt/its/data-converge/secret/root.p12"
TRUST_STORE_CERT_HASH_FILE="/tmp/root.db"

function file_comparison() {
  expected_md5=$(cat "$1" | awk '{print $1}')
  actual_md5=$(md5sum "$2" | awk '{print $1}')
  if [[ "$expected_md5" == "$actual_md5" ]]; then
      return 0
  else
      return 1
  fi
}

function do_service_check() {
  cat /tmp/$1 && rm -f /tmp/$1
  if [[  ! $? -eq 0  ]]; then
      log "error" "$1 is no exits"
      exit 2
  fi
  if [[ ! -e "$KEY_STORE_CERT_FILE" ]] || [[ ! -e "$TRUST_STORE_CERT_FILE" ]]; then
    log "error" "$KEY_STORE_CERT_FILE or $TRUST_STORE_CERT_FILE is no exits"
    exit 2
  fi

  if [[ ! -f "$KEY_STORE_CERT_HASH_FILE" ]]; then
    md5sum "$KEY_STORE_CERT_FILE" > "$KEY_STORE_CERT_HASH_FILE"
  fi
  if [[ ! -f "$TRUST_STORE_CERT_HASH_FILE" ]]; then
    md5sum "$TRUST_STORE_CERT_FILE" > "$TRUST_STORE_CERT_HASH_FILE"
  fi

  if ! file_comparison "$KEY_STORE_CERT_HASH_FILE" "$KEY_STORE_CERT_FILE" || ! file_comparison "$TRUST_STORE_CERT_HASH_FILE" "$TRUST_STORE_CERT_FILE"; then
    log "error" "certificates has change"
    exit 2
  fi
}

do_service_check health_tag
