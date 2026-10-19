#!/usr/bin/env bash
# Source this file to enable dconv in the current shell without starting service.

function _data_converge_append_command_path() {
    local command_dir=${BASH_SOURCE[0]}
    if [[ "${command_dir}" == */* ]]; then
        command_dir=${command_dir%/*}
    else
        command_dir=.
    fi
    command_dir=$(CDPATH='' cd -- "${command_dir}" && pwd) || return
    case ":${PATH-}:" in
        *":${command_dir}:"*) ;;
        *) PATH="${PATH:+${PATH}:}${command_dir}" ;;
    esac
    export PATH
}

_data_converge_append_command_path && unset -f _data_converge_append_command_path
