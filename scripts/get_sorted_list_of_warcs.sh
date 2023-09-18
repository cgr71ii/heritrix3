#!/bin/bash

WARCS_DIR="$1"
OUTPUT="$2"
WARCS_SED_EXPR="$3"
WARCS_SED_RESULT="$4"

if [[ ! -d "$WARCS_DIR" ]]; then
  >&2 echo "ERROR: Syntax: <warcs_dir> [<output>] [<warc_filename_prefix_sed_expr>] [<warc_filename_prefix_sed_expr_result>]"
  exit 2
fi

if [[ -z "$WARCS_SED_EXPR" ]]; then
  WARCS_SED_EXPR='warc_offset-([0-9]+)[.]gz'
fi

if [[ -z "$WARCS_SED_RESULT" ]]; then
  WARCS_SED_RESULT='warc_offset-\1.gz'
fi

if [[ -z "$OUTPUT" ]]; then
  OUTPUT="/dev/stdout"
elif [[ -f "$OUTPUT" ]]; then
  >&2 echo "WARNING: output already exists: sleeping 20s"
  sleep 20
fi

ls "$WARCS_DIR" \
  | sed -E 's/'"$WARCS_SED_EXPR"'/\1/' \
  | sort -g \
  | sed -E 's/^(.*)$/'"$WARCS_SED_RESULT"'/' \
  | xargs -I{} echo "$WARCS_DIR/{}" \
  > "$OUTPUT"
