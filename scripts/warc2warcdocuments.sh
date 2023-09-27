#/bin/bash

WARC="$1"
OUT_DIR_PREFIX="$2"

if [[ ! -f "$WARC" ]] || [[ -z "$OUT_DIR_PREFIX" ]]; then
  >&2 echo "ERROR: Syntax: <warc> <out_dir_prefix>"
  exit 1
fi

NUMBER_WARC=$(basename $WARC | sed -E 's/.*[0-9]{17}-([0-9]{5})-[0-9]+~.*/\10000/')

warcio index "$WARC" \
  | fgrep -a '"response"' \
  | sed -E 's/.*offset": "([0-9]+)".*/\1/' \
  | xargs -I{} bash -c 'printf "%015d\n" "{}"' \
  | xargs -I{} -P10 bash -c \
    'a=$(basename "{}"); \
     [[ ! -f "'"$OUT_DIR_PREFIX"'-'"$NUMBER_WARC"'{}.gz" ]] \
     && (warcio extract "'"$WARC"'" "{}" | pigz -c > '"$OUT_DIR_PREFIX"'-'"$NUMBER_WARC"'{}.gz) \
     || echo "Already exists: '"$OUT_DIR_PREFIX"'-'"$NUMBER_WARC"'{}.gz"'
