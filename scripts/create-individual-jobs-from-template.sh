#!/bin/bash

HERITRIX_URI="$1"
TEMPLATE_CONF="$2"
SEEDS_FILE="$3"
JOBS_OUTPUT_PREFIX="$4"

if [[ -z "$HERITRIX_URI" ]] || [[ ! -f "$TEMPLATE_CONF" ]] || ([[ ! -f "$SEEDS_FILE" ]] && [[ ! -p "$SEEDS_FILE" ]]) || [[ -z "$JOBS_OUTPUT_PREFIX" ]]; then
  >&2 echo "ERROR: syntax: <heritrix-uri> <template-conf> <seeds-file> <jobs-output-prefix>"

  exit 1
fi

HERITRIX_URI=$(echo "$HERITRIX_URI" | sed -E 's/\/+$//') # rstrip
heritrix_ok=$(curl -v -k -u admin:admin --anyauth --location "$HERITRIX_URI/engine" -H "Accept: application/xml" 2> /dev/null | fgrep -a "<heritrixVersion>" | wc -l)

if [[ "$heritrix_ok" == "0" ]]; then
  >&2 echo "Heritrix doesn't seem to be ok (have you specified the 'https?' scheme?): exit"

  exit 2
fi

uri_template="http://example.example/example"
uri_template_sed=$(echo "$uri_template" | sed 's/\//\\\//g')
check_template_uri_is_present=$(cat "$TEMPLATE_CONF" | egrep -a "^${uri_template}"$'\r'"?$" | wc -l)

if [[ "$check_template_uri_is_present" != "1" ]]; then
  >&2 echo "URI '$uri_template' was not found in template"

  exit 1
fi

template_filename=$(basename "$TEMPLATE_CONF")
heritrix_curl_out="${JOBS_OUTPUT_PREFIX}_heritrix_curl_out.log" # Can be changed to /dev/null or /dev/stdout

for seed in $(cat "$SEEDS_FILE"); do
  # Create job files
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
  output_conf_file="$output/$template_filename"

  echo "Seed output: $seed -> $output"

  mkdir -p "$output"

  if [[ -f "$output_conf_file" ]]; then
    >&2 echo "Conf. file already exists: $seed: skip $output_conf_file"

    continue
  fi

  cp "$TEMPLATE_CONF" "$output"

  sed -E -i "s/^${uri_template_sed}(\r?)$/$seed_sed\1/" "$output_conf_file"

  # REST API
  job_name=$(basename "$output")
  job_uri="$HERITRIX_URI/engine/job/$job_name"
  does_job_exist=$(curl -v -k -u admin:admin --anyauth --location -H "Accept: application/xml" "$job_uri" 2> /dev/null | fgrep -a "<shortName>${job_name}</shortName>" | wc -l)

  if [[ "$does_job_exist" == "1" ]]; then
    >&2 echo "Job already exists in Heritrix: skip $job_name"

    continue
  fi

  echo "[$(date +%Y%m%d%H%M%S)] Add job: $job_name" &>> "$heritrix_curl_out"
  curl -v -d "action=add&addpath=$output" -k -u admin:admin --anyauth --location "$HERITRIX_URI/engine" &>> "$heritrix_curl_out" # Add job (results will be available in the job directory, not in heritrix's)
  echo "[$(date +%Y%m%d%H%M%S)] Launch job: $job_name" &>> "$heritrix_curl_out"
  curl -v -d "action=launch" -k -u admin:admin --anyauth --location -H "Accept: application/xml" "$job_uri" &>> "$heritrix_curl_out" # Launch job
  echo "[$(date +%Y%m%d%H%M%S)] Unpause job: $job_name" &>> "$heritrix_curl_out"
  curl -v -d "action=unpause" -k -u admin:admin --anyauth --location -H "Accept: application/xml" "$job_uri" &>> "$heritrix_curl_out" # Unpause job
  echo "[$(date +%Y%m%d%H%M%S)] Job should be running: $job_name" &>> "$heritrix_curl_out"
  echo &>> "$heritrix_curl_out"
done
