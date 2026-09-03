#!/usr/bin/env bash

# Shared .env reading and --config handling for the proxies, sourced rather
# than executed. A --config naming a *.example.yaml template is materialized
# next to it from .env, and every other --config resolves to an absolute path.

REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${REPO_DIR}/.env"

# resolved carries the path resolve_config produced. A global rather than
# stdout, so a failure exits the script instead of a subshell.
resolved=""

# env_names and env_values carry .env in file order, and env_value carries the
# entry env_lookup found.
env_names=()
env_values=()
env_value=""

trim() {
    local text="${1#"${1%%[![:space:]]*}"}"
    printf '%s' "${text%"${text##*[![:space:]]}"}"
}

# read_env_file parses .env as text, so no value is expanded or executed.
# Comments, blank lines, and lines without an assignment are skipped.
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
# name is absent. A name repeated in .env keeps its last value.
env_lookup() {
    local index
    env_value=""
    for (( index = 0; index < ${#env_names[@]}; index++ )); do
        if [[ "${env_names[index]}" == "$1" ]]; then
            env_value="${env_values[index]}"
        fi
    done
}

# absolute leaves an absolute path in resolved. A path that cannot be resolved
# stops the run instead of reaching the CLI.
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
# left alone. An unset or empty variable stops the run.
materialize() {
    local template="$1"
    local generated="${template%.example.yaml}.yaml"
    if ! command -v envsubst > /dev/null; then
        echo "error: envsubst is required, install gettext" >&2
        exit 1
    fi

    # provoor expands nothing, so an unbraced reference reaches the cluster
    # verbatim.
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

    # env carries only the names the template uses, so no ambient variable
    # reaches envsubst and no .env name lands in this shell.
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
# value and passing everything else through.
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
