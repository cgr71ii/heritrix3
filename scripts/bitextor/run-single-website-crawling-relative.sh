#!/bin/bash

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

log_file="$1"
lang_pair="$2" # e.g. en-is
experiment="$3" # e.g. crawling_20230906145856_yes_cld2_no_classifier
path_to_experiment="$4" # e.g. /home/cgarcia/Documentos/heritrix3/experiments
experiments_reference="$5" # e.g. "crawling_20230909172127_yes_cld2_yes_classifier crawling_20230906145856_yes_cld2_no_classifier"
# Bitextor extra args (see error message for an example or $bitextor_script):
LANG1="$6"
LANG2="$7"
TRANSLATION_DIRECTION="$8"
TRANSLATION_SCRIPT="$9"
BICLEANER_AI_YAML="${10}"
BITEXTOR_FORCE="${11}"
WORK_DIR_SUFFIX="${12}"

batch_size="10" # percentage
bitextor_parallel_runs="10"
work_dir="work_${experiment}_${lang_pair}"
#bitextor_script="$DIR/run-bitextor-${lang_pair}-work_dir.sh"
bitextor_script="$DIR/run-bitextor-extra-args-relative.sh"
bitextor_loader="srun --gres=gpu:1 --cpus-per-task=2 --mem-per-cpu=3G"

if [[ ! -z "$WORK_DIR_SUFFIX" ]]; then
  work_dir="${work_dir}_$WORK_DIR_SUFFIX"
fi

CURRENT_DATE=$(date +%Y%m%d_%s)

if [[ -z "$log_file" ]] || [[ -f "$log_file" ]]; then
  >&2 echo "ERROR: log file empty or already exists: $log_file"
  exit 1
fi
if [[ ! -f "$bitextor_script" ]]; then
  >&2 echo "ERROR: bitextor script not found: $bitextor_script"
  exit 1
fi
if [[ "$batch_size" -gt "100" ]] || [[ "$batch_size" -le "0" ]]; then
  >&2 echo "ERROR: batch size is a percentage > 0, but got $batch_size"
  exit 1
fi

if [[ "$(command -v srun)" == "" ]]; then
  bitextor_parallel_runs="1" # Can be modified if the provided bitextor script does not need to allocate resources like GPU
  bitextor_loader=""
  >&2 echo "WARNING: srun not available: bitextor will not be scheduled: bitextor will be executed with $bitextor_parallel_runs run/s in parallel"
  sleep 10
fi

batch_size_mod=$((batch_size*bitextor_parallel_runs))

if [[ -z "$experiments_reference" ]]; then
  >&2 echo "ERROR: experiments reference is mandatory in order to know the max. quantity of docs to process per website"
  exit 1
else
  for experiment_reference in $(echo "$experiments_reference"); do
    reference=$(ls $path_to_experiment/$experiment_reference/$lang_pair/*/latest/warcs/warcs_path.abs_path 2> /dev/null | wc -l)

    if [[ "$reference" == "0" ]]; then
      >&2 echo "ERROR: provided experiment reference could not be found"
      exit 1
    fi
  done
fi

get_documents () {
  local docs_permanent="$1"

  if [[ ! -d "$docs_permanent" ]] || [[ ! -f "$(ls ${docs_permanent}/??-??.documents.gz 2> /dev/null)" ]]; then
    >&2 echo "INFO: break: no permanent dir or no document.gz file: $docs_permanent"
    echo "-1"

    return
  fi

  no_docs=$(zcat ${docs_permanent}/??-??.documents.gz 2> /dev/null | tail -n +2 | wc -l) # tail: remove header

  if [[ "$no_docs" -le "0" ]]; then
    >&2 echo "INFO: break: 0 docs: $docs_permanent"
  fi

  echo "$no_docs"
}

for warcs_path_file in $(ls $path_to_experiment/$experiment/$lang_pair/*/latest/warcs/warcs_path.abs_path); do
  jobn=$(basename $(dirname $(dirname $(dirname "$warcs_path_file"))))
  website=$(echo "$jobn" | sed -E 's/.*(https?[^\/]*)/\1/')
  documents2process="0"
  dirn=$(dirname "$warcs_path_file")
  available_documents=$(cat "$warcs_path_file" | wc -l)
  ids_to_process=()

  while [[ "$documents2process" -lt "$available_documents" ]]; do
    documents2process=$((documents2process+10000))
  done

  if [[ "$reference" != "0" ]] && [[ ! -z "$website" ]]; then
    # Try to find the appropiate number of documents to process
    continue_var="0"

    for experiment_reference in $(echo "$experiments_reference"); do
      reference=$(ls $path_to_experiment/$experiment_reference/$lang_pair/*/latest/warcs/warcs_path.abs_path 2> /dev/null | wc -l)
      reference_file_wc=$(ls $path_to_experiment/$experiment_reference/$lang_pair/*${website}*/latest/warcs/warcs_path.abs_path | wc -l)

      if [[ "$reference_file_wc" == "1" ]]; then
        aux_documents2process=$(cat $path_to_experiment/$experiment_reference/$lang_pair/*${website}*/latest/warcs/warcs_path.abs_path | wc -l)

        if [[ ! -z "$aux_documents2process" ]]; then
          if [[ "$aux_documents2process" -ge "$documents2process" ]]; then
            aux_documents2process="$documents2process" # Min. number of documents to process
          fi

          echo "Documents to process: $jobn ($experiment_reference): $documents2process -> $aux_documents2process"

          documents2process="$aux_documents2process"
        else
          >&2 echo "WARNING: could not GET appropiate number of documents to process (continue): $reference - $website"
          continue_var="1"
          break
        fi
      else
        >&2 echo "WARNING: could not FIND appropiate number of documents to process (continue): $reference - $reference_file_wc - $website"
        continue_var="1"
        break
      fi
    done

    if [[ "$continue_var" == "1" ]]; then
      continue
    fi
  fi

  echo "Start: $(date): $warcs_path_file"

  for n in $(seq -s ' ' 100 -$batch_size 1); do
    warcs_dir="${work_dir}/${jobn}/${n}_percentage_warcs_processed"
    docs_permanent="${warcs_dir}/permanent"
    m=$(echo "($n - $batch_size) % $batch_size_mod" | bc)
    currentdocs2process=$(echo "scale=2; $documents2process * ($n / 100) + 0.5" | bc) # +0.5 -> round
    currentdocs2process=$(echo "$currentdocs2process / 1" | bc) # Remove decimals

    # Already executed? If executed and no parallel documents are found, current website will not have additional executions
    if [[ -d "$warcs_dir" ]]; then
      echo "continue (perhaps break): warcs dir already exists: $jobn"

      no_docs=$(get_documents "$docs_permanent")

      if [[ "$no_docs" -le "0" ]]; then
        break
      fi

      continue
    fi

    # Run bitextor
    mkdir -p "$warcs_dir"

    (echo "$jobn - $currentdocs2process ($n %) - $(date)"
    $bitextor_loader $bitextor_script "$jobn" "$warcs_path_file" "$currentdocs2process" "$work_dir" "$LANG1" "$LANG2" "$TRANSLATION_DIRECTION" "$TRANSLATION_SCRIPT" "$BICLEANER_AI_YAML" "$BITEXTOR_FORCE" "$n" &>> "$warcs_dir/bitextor_run_${CURRENT_DATE}.log"
    echo "Done - $jobn - $currentdocs2process - ($n %) - $(date)") &
    ids_to_process+=("$n")

    # Wait and, optionally, break?
    if [[ "$m" == "0" ]]; then
      wait

      break_again="false"

      for n2 in "${ids_to_process[@]}"; do
        warcs_dir="${work_dir}/${jobn}/${n2}_percentage_warcs_processed"
        docs_permanent="${warcs_dir}/permanent"

        rm -rf "${work_dir}/${jobn}/${n2}_percentage_warcs_processed/.snakemake"

        no_docs=$(get_documents "$docs_permanent")

        if [[ "$no_docs" -le "0" ]]; then
          break_again="true"
        fi
      done

      ids_to_process=()

      if [[ "$break_again" == "true" ]]; then
        break
      fi
    fi
  done

  wait

  # Unprocessed
  for n2 in "${ids_to_process[@]}"; do
    warcs_dir="${work_dir}/${jobn}/${n2}_percentage_warcs_processed"
    docs_permanent="${warcs_dir}/permanent"

    rm -rf "${work_dir}/${jobn}/${n2}_percentage_warcs_processed/.snakemake"
  done

  echo "End: $(date): $warcs_path_file"
done &>> "$log_file"
