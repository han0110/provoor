#!/usr/bin/env bash

# Shared .env reading and --config handling for the provoor and benchmarkoor
# proxies, sourced rather than executed. A --config naming a *.example.yaml
# template is materialized next to the template with every placeholder filled
# from .env, and the generated file takes its place in the argument list.
# Every other --config resolves to an absolute path, so a proxy is free to
# change directory before running its CLI.

REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${REPO_DIR}/.env"

# resolved carries the path resolve_config produced. A global rather than
# stdout, so a failure exits the script instead of a substitution subshell.
resolved=""

# env_names and env_values carry .env in file order, env_value the entry
# env_lookup found. Globals for the same reason resolved is one.
env_names=()
env_values=()
env_value=""

trim() {
    local text="${1#"${1%%[![:space:]]*}"}"
    printf '%s' "${text%"${text##*[![:space:]]}"}"
}

# read_env_file parses .env as text, so no value is ever expanded, executed,
# or spread over more than the line it sits on. A comment at any indentation,
# a blank line, and a line without an assignment are skipped, and the last
# line counts whether or not a newline terminates it.
read_env_file() {
    if [[ ! -f "${ENV_FILE}" ]]; then
        echo "error: missing ${ENV_FILE}, copy .env.example and fill it in" >&2
        exit 1
    fi

    env_names=()
    env_values=()
    local line key value
    while IFS= read -r line || [[ -n "${line}" ]]; do
        line="$(trim "${line%$'\r'}")"
        [[ -z "${line}" || "${line}" == '#'* ]] && continue
        [[ "${line}" == *=* ]] || continue
        key="$(trim "${line%%=*}")"
        [[ -n "${key}" ]] || continue
        value="$(trim "${line#*=}")"
        if (( ${#value} >= 2 )) && [[ "${value}" == \"*\" || "${value}" == \'*\' ]]; then
            value="${value:1:${#value}-2}"
        fi
        # Whitespace alone stands for no value, so it never fills a field.
        [[ -n "$(trim "${value}")" ]] || value=""
        env_names+=("${key}")
        env_values+=("${value}")
    done < "${ENV_FILE}"
}

# env_lookup leaves the value .env gives a name in env_value, empty when the
# name is absent, so only .env can satisfy a placeholder. A name repeated in
# .env keeps its last value.
env_lookup() {
    local index
    env_value=""
    for (( index = 0; index < ${#env_names[@]}; index++ )); do
        if [[ "${env_names[index]}" == "$1" ]]; then
            env_value="${env_values[index]}"
        fi
    done
}

# absolute leaves an absolute path in resolved. The path travels after --, so
# a leading dash names a file rather than an option, and a path that cannot be
# resolved stops the run instead of reaching the CLI as a directory.
absolute() {
    local directory name
    if ! directory="$(dirname -- "$1")" \
        || ! name="$(basename -- "$1")" \
        || ! directory="$(cd -- "${directory}" && pwd)"; then
        echo "error: cannot resolve config: $1" >&2
        exit 1
    fi
    resolved="${directory}/${name}"
}

# materialize substitutes by variable name, so text outside a placeholder is
# left alone, and an unset or empty variable stops the run rather than
# silently producing an empty field.
materialize() {
    local template="$1"
    local generated="${template%.example.yaml}.yaml"
    if ! command -v envsubst > /dev/null; then
        echo "error: envsubst is required, install gettext" >&2
        exit 1
    fi

    # provoor's loader expands nothing, so an unbraced reference would reach
    # the cluster verbatim instead of carrying an address.
    local unbraced=()
    mapfile -t unbraced < <(grep -oE '\$[A-Za-z_][A-Za-z0-9_]*' "${template}" | sort -u)
    if (( ${#unbraced[@]} > 0 )); then
        echo "error: ${template} has unbraced ${unbraced[*]}, write \${NAME}" >&2
        exit 1
    fi

    read_env_file

    local names=() assignments=() missing=() name
    # shellcheck disable=SC2016 # grep and tr take literal patterns.
    while read -r name; do
        [[ -z "${name}" ]] && continue
        env_lookup "${name}"
        if [[ -z "${env_value}" ]]; then
            missing+=("${name}")
            continue
        fi
        names+=("\$${name}")
        assignments+=("${name}=${env_value}")
    done < <(grep -oE '\$\{[A-Za-z0-9_]+\}' "${template}" | tr -d '${}' | sort -u)
    if (( ${#missing[@]} > 0 )); then
        echo "error: ${ENV_FILE} does not set ${missing[*]}" >&2
        exit 1
    fi

    # env carries the names the template uses and nothing else, so neither an
    # ambient variable nor an unrelated .env entry reaches envsubst, and no
    # name from .env lands in this shell.
    env "${assignments[@]+"${assignments[@]}"}" \
        envsubst "${names[*]}" < "${template}" > "${generated}"
    absolute "${generated}"
}

resolve_config() {
    if [[ ! -f "$1" ]]; then
        echo "error: no such config: $1" >&2
        exit 1
    fi
    case "$1" in
        *.example.yaml)
            materialize "$1"
            ;;
        *)
            absolute "$1"
            ;;
    esac
}

# rewrite_config_args fills the global args array, resolving every --config
# value and passing everything else through untouched.
rewrite_config_args() {
    args=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --config)
                args+=("$1")
                shift
                if (( $# == 0 )); then
                    echo "error: --config needs a path" >&2
                    exit 1
                fi
                resolve_config "$1"
                args+=("${resolved}")
                ;;
            --config=*)
                resolve_config "${1#--config=}"
                args+=("--config=${resolved}")
                ;;
            *)
                args+=("$1")
                ;;
        esac
        shift
    done
}
