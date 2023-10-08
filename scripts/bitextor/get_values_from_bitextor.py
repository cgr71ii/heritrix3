
import os
import sys

batch_size = 50

lang_pairs = sys.argv[1]
bitextor_permanent_file = sys.argv[2]
how_many_cols = int(sys.argv[3])

before_dynamic_cols_fields = 4

col_desc = sys.argv[before_dynamic_cols_fields + how_many_cols * 0:before_dynamic_cols_fields + how_many_cols * 1]
path_templates = sys.argv[before_dynamic_cols_fields + how_many_cols * 1:before_dynamic_cols_fields + how_many_cols * 2] # e.g., /path/to/crawl/files/template_{}/latest/warcs/warcs_path.abs_path # {} is the website provided in stdin
path_bitextor = sys.argv[before_dynamic_cols_fields + how_many_cols * 2:before_dynamic_cols_fields + how_many_cols * 3] # e.g., /path/to/bitextor/files/template_{}/[]_warcs_processed/permanent/lang1-lang2.deduped.txt.gz # [] is the batch size

expected_args = how_many_cols * 3 + before_dynamic_cols_fields

if len(sys.argv) != expected_args:
  raise Exception(f"Unexpected len of args: {len(sys.argv)} vs {expected_args}")

if len(path_templates) != len(path_bitextor):
  raise Exception(f"Diff len: {len(path_templates)} vs {len(path_bitextor)}")

_bitextor_supported_files = ("deduped.txt.gz", "sent.gz", "documents.gz")

if bitextor_permanent_file not in _bitextor_supported_files:
  if ".gz.custom_" in bitextor_permanent_file:
    sys.stderr.write(f"INFO: custom permanent file: {bitextor_permanent_file}\n")
  else:
    raise Exception(f"Unsupported bitextor file: {bitextor_permanent_file}: supoprted files: {_bitextor_supported_files}")

for path_template, path_b in zip(path_templates, path_bitextor):
  if not path_template.endswith("latest/warcs/warcs_path.abs_path"):
    raise Exception(f"Path does not ends with 'latest/warcs/warcs_path.abs_path': {path_template}")
  if not path_b.endswith(f"{lang_pairs}.{bitextor_permanent_file}"):
    raise Exception(f"Path does not ends with '{bitextor_permanent_file}': {path_b}")
  if path_template.find("{}") < 0:
    raise Exception(f"{{}} not found in path: {path_template}")
  if path_b.find("{}") < 0 or path_b.find("[]") < 0:
    raise Exception(f"{{}} or [] not found in path: {path_b}")

header1 = '\t'.join([f"actual_docs_{c}" for c in col_desc])
header2 = '\t'.join([f"parallel_docs_{c}" for c in col_desc])

print(f"website\tprocessed_warcs\t{header1}\t{header2}") # Header

BITEXTOR_SCRIPT_FILTER_CMD_ENVVAR = "cat"

if "BITEXTOR_SCRIPT_FILTER_CMD_ENVVAR" in os.environ:
  BITEXTOR_SCRIPT_FILTER_CMD_ENVVAR = os.environ("BITEXTOR_SCRIPT_FILTER_CMD_ENVVAR")

  sys.stderr.write(f"INFO: custom filtering cmd provided: {BITEXTOR_SCRIPT_FILTER_CMD_ENVVAR}\n")

for website in sys.stdin:
  website = website.rstrip("\r\n")
  fail = False

  for paths in (path_templates, path_bitextor):
    for path_template in paths:
      path = '/'.join(path_template.replace("{}", website).split('/')[:-3])

      if not os.path.exists(path):
        sys.stderr.write(f"File does not exist: {path} (website: {website})\n")
        fail = True
        continue

  if fail:
    continue

  max_warcs = []

  for path_template in path_templates:
    path = path_template.replace('{}', website)
    max_warcs.append(0)
    with open(path) as fd:
      for l in fd:
        max_warcs[-1] += 1
      max_warcs[-1] += batch_size # We add batch_size to later check <= with batch_size

    max_warcs_max = sorted(max_warcs)[-1] # Max value

  c = batch_size
  found = [False for _ in path_templates]
  docs = [0 for _ in path_templates]
  prev_docs = [0 for _ in path_templates]
  par = ['0' for _ in path_templates]

  while c < max_warcs_max:
    d = []
    f = []

    for idx, path_b in enumerate(path_bitextor):
      path = path_b.replace("{}", website).replace("[]", str(c))
      dire = path.replace(f"/permanent/{lang_pairs}.{bitextor_permanent_file}", '')

      d.append(dire)
      f.append(path)

      if os.path.exists(d[idx]):
        found[idx] = True

      docs[idx] = min(max_warcs[idx] - batch_size, c) # Correct available document

      if os.path.exists(d[idx]):
        if os.path.isfile(f[idx]):
          par[idx] = str(int(os.popen(f"zcat {f[idx]} | tail -n +2 | {BITEXTOR_SCRIPT_FILTER_CMD_ENVVAR} | wc -l").read().rstrip("\r\n")))
        else:
          par[idx] = '0'
      elif found[idx]:
        par[idx] = '-'

    _docs = '\t'.join(list(map(str, docs)))
    _par = '\t'.join(par)

    print(f"{website}\t{c}\t{_docs}\t{_par}")

    prev_docs = [d for d in docs]
    c += batch_size
