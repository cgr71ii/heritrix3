#!/bin/bash

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

syntax_error() {
  >&2 echo "Syntax: <experiment_name> <experiment_warcs_dir_or_file> <quantity_warcs_to_process> <work_dir> <lang1> <lang2> <translation_direction> <translation_bash_script_with_options> <bicleaner_ai_metadata_file>"

  exit 1
}

EXPERIMENT_NAME="$1"
EXPERIMENT_WARCS_DIR_OR_FILE="$2"
WARCS_TO_PROCESS="$3"
WORK_DIR="$4"
LANG1="$5" # e.g. en
LANG2="$6" # e.g. is
TRANSLATION_DIRECTION="$7" # e.g. is2en
TRANSLATION_SCRIPT="$8" # e.g. bash /home/cgarcia/Documentos/marian-dev/scripts/marian-translate-is2en.sh 4
BICLEANER_AI_YAML="$9" # e.g. /home/cgarcia/bicleaner-ai-model/en-is/metadata.yaml

CURRENT_DATE=$(date +%Y%d%m_%s)

if [[ -z "$EXPERIMENT_NAME" ]] || [[ -z "$EXPERIMENT_WARCS_DIR_OR_FILE" ]] || ([[ ! -d "$EXPERIMENT_WARCS_DIR_OR_FILE" ]] && [[ ! -f "$EXPERIMENT_WARCS_DIR_OR_FILE" ]]) || \
   [[ -z "$WARCS_TO_PROCESS" ]] || [[ ! "$WARCS_TO_PROCESS" =~ ^[0-9]+$ ]] || [[ -z "$WORK_DIR" ]] || \
   [[ ! "$LANG1" =~ ^[a-z][a-z]$ ]] || [[ ! "$LANG2" =~ ^[a-z][a-z]$ ]] || [[ -z "$TRANSLATION_SCRIPT" ]] || \
   [[ ! "$TRANSLATION_DIRECTION" =~ ^[a-z]2[a-z]$ ]] || [[ ! -f "$BICLEANER_AI_YAML" ]]; then
  syntax_error
fi

WORK="${DIR}/${WORK_DIR}/${EXPERIMENT_NAME}/${WARCS_TO_PROCESS}_warcs_processed"

if [[ -d "$WORK" ]]; then
  >&2 echo "WORK directory already exists. You might want to remove or move: $WORK ; Sleeping 20 senconds..."

  sleep 20
fi

NO_WARCS=$([[ -d "$EXPERIMENT_WARCS_DIR_OR_FILE" ]] && ls "${EXPERIMENT_WARCS_DIR_OR_FILE}" | grep -a "[.]warc[.]gz$" | wc -l || cat "$EXPERIMENT_WARCS_DIR_OR_FILE" | wc -l)

if [[ "$NO_WARCS" == "0" ]]; then
  >&2 echo "No WARCs found in $EXPERIMENT_WARCS_DIR_OR_FILE"

  exit 1
fi

if [[ "$WARCS_TO_PROCESS" -gt "$NO_WARCS" ]]; then
  >&2 echo "WARCs to process > found WARCs = $WARCS_TO_PROCESS > $NO_WARCS ; Sleeping 20 seconds..."

  sleep 20
fi

mkdir -p "$WORK"

#WARCS_FILES=$(mktemp /tmp/puc_crawling_experiment_bitextor.XXXXXX)
WARCS_FILES="${WORK}/warcs.list"
REPORT_FILE="${WORK}/report_${CURRENT_DATE}.txt"
TMP_WARC="${WORK}/allwarcs2singlewarc.gz"

if [[ ! -f "$WARCS_FILES" ]]; then
  echo "Creating WARCS_FILES ..."

  if [[ -f "$EXPERIMENT_WARCS_DIR_OR_FILE" ]]; then
#    cat "$EXPERIMENT_WARCS_DIR_OR_FILE" | head -n "$WARCS_TO_PROCESS" > "$WARCS_FILES"
    cat "$EXPERIMENT_WARCS_DIR_OR_FILE" | head -n "$WARCS_TO_PROCESS" | xargs -I{} cat {} > "$TMP_WARC"
    echo "$TMP_WARC" > "$WARCS_FILES"
  else
    ls "${EXPERIMENT_WARCS_DIR_OR_FILE}"/*.gz | sort | head -n "$WARCS_TO_PROCESS" > "$WARCS_FILES"
  fi
fi

echo "WORK: $WORK"
echo "WARCS_FILES: $WARCS_FILES"
echo "NO_WARCS: $NO_WARCS"
echo "REPORT_FILE: $REPORT_FILE"

echo "Running..."

pushd "$WORK" > /dev/null

bitextor --notemp -j 8 \
            --config profiling=True permanentDir="${WORK}/permanent" dataDir="${WORK}/data" transientDir="${WORK}/transient" \
                warcsFile="$WARCS_FILES" shards=8 batches=2048 lang1="$LANG1" lang2="$LANG2" \
                documentAligner="externalMT" alignerCmd="$TRANSLATION_SCRIPT" translationDirection="$TRANSLATION_DIRECTION" sentenceAligner="bleualign" \
                bifixer=True bicleaner=True bicleanerFlavour="ai" bicleanerModel="$BICLEANER_AI_YAML" \
                deferred=True tmx=True boilerplateCleaning=False deduped=True paragraphIdentification=True additionalMetadata=True bicleanerExtraArgs="--disable_minimal_length" \
                bicleanerThreshold=0.5 granularity='["sentences", "documents"]' \
            &> "$REPORT_FILE"

echo "Done! Exit status: $?"

popd > /dev/null

rm -f "$TMP_WARC"
