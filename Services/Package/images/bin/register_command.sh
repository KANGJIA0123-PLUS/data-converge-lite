#!/usr/bin/env bash
# Register the operator command for this account's future Bash sessions.
set -eu

function registration_error() {
    printf 'Cannot register dconv shell environment: %s\n' "$1" >&2
    exit 1
}

function append_profile_hook() {
    local profile=$1
    local search_status
    if [[ -L "${profile}" || ( -e "${profile}" && ! -f "${profile}" ) ]]; then
        registration_error "not a regular non-symlink file: ${profile}"
    fi
    if [[ -e "${profile}" ]]; then
        [[ -r "${profile}" && -O "${profile}" ]] || registration_error "not readable or not owned by this account: ${profile}"
        if grep -Fqx -- "${profile_hook}" "${profile}"; then
            return
        else
            search_status=$?
            [[ ${search_status} -eq 1 ]] || registration_error "cannot inspect: ${profile}"
        fi
        [[ -w "${profile}" ]] || registration_error "not writable: ${profile}"
    fi
    # Add a newline even if the user's existing final line has none.
    (umask 077; printf '\n%s\n' "${profile_hook}" >> "${profile}") || registration_error "cannot append: ${profile}"
}

[[ -n "${HOME:-}" && -d "${HOME}" && -w "${HOME}" ]] || registration_error 'HOME is missing or not writable'
script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
helper="${script_dir}/command_env.sh"
[[ -r "${helper}" ]] || registration_error "cannot read: ${helper}"
printf -v profile_hook 'if [ -n "${BASH_VERSION:-}" ] && [ -r %q ]; then . %q; fi # data-converge command environment' "${helper}" "${helper}"

# Bash uses the first readable login file; never create a higher-priority file.
login_profile=
existing_profile=false
for profile_name in .bash_profile .bash_login .profile; do
    candidate="${HOME}/${profile_name}"
    if [[ -e "${candidate}" || -L "${candidate}" ]]; then
        existing_profile=true
        if [[ -r "${candidate}" ]]; then
            login_profile=${candidate}
            break
        fi
    fi
done
if [[ -z "${login_profile}" ]]; then
    [[ "${existing_profile}" == false ]] || registration_error 'no readable login profile'
    login_profile="${HOME}/.profile"
fi

append_profile_hook "${HOME}/.bashrc"
append_profile_hook "${login_profile}"
