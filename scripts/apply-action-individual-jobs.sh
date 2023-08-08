#!/bin/bash

HERITRIX_URI="$1"
SEEDS_FILE="$2"
JOBS_OUTPUT_PREFIX="$3"
ACTION="$4"

if [[ -z "$HERITRIX_URI" ]] || ([[ ! -f "$SEEDS_FILE" ]] && [[ ! -p "$SEEDS_FILE" ]]) || [[ -z "$JOBS_OUTPUT_PREFIX" ]] || [[ -z "$ACTION" ]]; then
  >&2 echo "ERROR: syntax: <heritrix-uri> <seeds-file> <jobs-output-prefix> <action>"

  exit 1
fi

if [[ "$ACTION" != "build" ]] && [[ "$ACTION" != "launch" ]] && [[ "$ACTION" != "pause" ]] && [[ "$ACTION" != "unpause" ]] && [[ "$ACTION" != "terminate" ]] && [[ "$ACTION" != "teardown" ]] && [[ "$ACTION" != "checkpoint" ]]; then
  >&2 echo "Unknown specified action: $ACTION"

  exit 3
fi

HERITRIX_URI=$(echo "$HERITRIX_URI" | sed -E 's/\/+$//') # rstrip
heritrix_ok=$(curl -v -k -u admin:admin --anyauth --location "$HERITRIX_URI/engine" -H "Accept: application/xml" 2> /dev/null | fgrep -a "<heritrixVersion>" | wc -l)

if [[ "$heritrix_ok" == "0" ]]; then
  >&2 echo "Heritrix doesn't seem to be ok (have you specified the 'https?' scheme?): exit"

  exit 2
fi

heritrix_curl_out="${JOBS_OUTPUT_PREFIX}_heritrix_curl_out_action_$ACTION.log" # Can be changed to /dev/null or /dev/stdout

for seed in $(cat "$SEEDS_FILE"); do
  seed=$(echo "$seed" | sed -E 's/\/+$//') # rstrip slash
  seed_has_scheme=$(echo "$seed" | fgrep -a "://" | wc -l)
  seed_scheme="http" # default scheme if none is provided
  seed_wo_scheme="$seed"

  if [[ "$seed_has_scheme" == "1" ]]; then
    seed_scheme=$(echo "$seed" | sed -E 's/^([^(://)]*):\/\/.*$/\1/')
    seed_wo_scheme=$(echo "$seed" | sed -E 's/^[^(://)]*:\/\/(.*)$/\1/')
  fi

  seed="${seed_scheme}://${seed_wo_scheme}"
  seed_sed=$(echo "$seed" | sed 's/\//\\\//g')
  seed_output=$(echo "$seed" | tr '/' 'S' | tr '>' 'G' | tr '<' 'L' | tr '|' 'P' | tr '&' 'A' | tr ':' 'D') # no special characters in filenames
  output="${JOBS_OUTPUT_PREFIX}_seed_${seed_output}"

  # REST API
  job_name=$(basename "$output")
  job_uri="$HERITRIX_URI/engine/job/$job_name"
  does_job_exist=$(curl -v -k -u admin:admin --anyauth --location -H "Accept: application/xml" "$job_uri" 2> /dev/null | fgrep -a "<shortName>${job_name}</shortName>" | wc -l)

  if [[ "$does_job_exist" == "0" ]]; then
    >&2 echo "Job does not exist in Heritrix: skip $job_name"

    continue
  fi

  echo "[$(date +%Y%m%d%H%M%S)] $ACTION: job $job_name" &>> "$heritrix_curl_out"
  curl -v -d "action=$ACTION" -k -u admin:admin --anyauth --location -H "Accept: application/xml" "$job_uri" &>> "$heritrix_curl_out" # Launch job
  echo &>> "$heritrix_curl_out"

  echo "$ACTION -> $seed -> $job_name"
done
