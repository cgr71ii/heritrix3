#!/bin/bash

d=$(date +%Y%m%d%H%M%S)
DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

config_dir="$1"
langs="$2"
output_prefix="$3" # e.g. /home/cgarcia/crawling-puc

if [[ -z "$config_dir" ]]; then
  >&2 echo "Syntax error"
  exit 1
fi
if [[ -z "$langs" ]]; then
  langs="en-is en-fi en-mt es-eu"
fi
if [[ ! -d "$output_prefix" ]]; then
  >&2 echo "Output directory does not exist"
fi

echo "Langs: $langs"

heritrix_dir="$DIR/.."

for l in $(echo "$langs"); do
  o="$output_prefix/$config_dir/$l"
  f_wc=$(ls -d $o/*_$l* 2> /dev/null | egrep -a "/[0-9]+_$l" | wc -l)

  if [[ "$f_wc" != "1" ]]; then
    echo "Could not found output dir: $o ($f_wc)"
    continue
  fi

  f=$(ls -d $o/*_$l* | sed -E 's/^.*\/([0-9]+_'$l').*$/'$o'\/\1/')
  log="/tmp/heritrix_teardown_${l}_${d}.log"

  $heritrix_dir/scripts/apply-action-individual-jobs.sh \
    "https://localhost:8443" \
    $heritrix_dir/crawling-experiments/crawling-seeds/${l}.tmx.gz.urls.fixes_and_same_domain.same_authority_sort_uniqc.authorities.shuf.without_domains_from_puc_nor_url2lang_train_and_dev.200_and_not_redirect.out.sort_by_uniq_pairs_of_parallel_docs.seeds_200 \
    $f \
    teardown &> "$log" &

  echo "Log: $l: $log"
done

echo "Waiting"
wait
echo "Done!"
