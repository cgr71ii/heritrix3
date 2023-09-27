#!/bin/bash

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

PREFIX="$1" # e.g. /path/to/heritrix/jobs or '/path/to/heritrix/jobs/??-??' (pattern: $PREFIX/*/latest)

if [[ "$PREFIX" != /* ]]; then
  >&2 echo "ERROR: provided PREFIX has to be absolute, not relative"
  exit 1
fi

t=$(ls -d "$PREFIX"/*/latest | wc -l)

if [[ "$t" == "0" ]]; then
  >&2 echo "ERROR: provided PREFIX is not valid"
  exit 1
fi

date
ls $PREFIX/*/latest/warcs/*.warc.gz* \
  | xargs -I{} -P10 bash -c 'a=$(dirname "{}"); b="$a/documents"; [[ ! -d "$b" ]] && mkdir "$b"; prefix="$b/warc_offset"; '$DIR'/warc2warcdocuments.sh "{}" "$prefix"' &> $PREFIX/warcs.log \
  && echo "ok1 - $(date)" \
  && ls -d $PREFIX/*/latest/warcs/documents \
    | xargs -I{} -P10 bash -c ''$DIR'/get_sorted_list_of_warcs.sh "{}" > {}/../warcs_path.abs_path' \
    && echo "ok2 - $(date)"
