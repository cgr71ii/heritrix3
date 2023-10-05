
import sys
import base64
#import subprocess
#import shlex

from pytools import persistent_dict

# This script stores in disk-based the translations which can be exploited in other translation executions, and print all the translations in order
# Expected to be used in the translation script in bitextor: ... | translation_script.sh | python3 this_script.py <storage> <tmp_file>

debug = False
#debug = True

id_storage = base64.b64encode(sys.argv[1].encode()).decode() # Optional separate or common storage
tmp_file = sys.argv[2]

storage = persistent_dict.PersistentDict(id_storage)
translations = []

for l in sys.stdin:
  translation = l.rstrip("\r\n ")
  translations.append(l)

if len(translations) > 0:
  # The tmp file should be closed
  translations = translations[:-1]
else:
  sys.stderr.write("WARNING: did not get the expected control line\n")

tmp_file_fd = open(tmp_file, "r")
current_translation = 0

for idx, l in enumerate(tmp_file_fd, 1):
  translation_available, line_idx, translation_or_hash = l.rstrip("\r\n ").split('\t')[:3]

  if translation_available == "1":
    translation = translation_or_hash.rstrip("\r\n ")

    if debug:
      sys.stderr.write(f"Line #{idx} (idx: {line_idx}): was cached\n")
  else: # "0"
    translation = translations[current_translation].rstrip("\r\n ")

    current_translation += 1

    storage.store(translation_or_hash, translation)

    if debug:
      sys.stderr.write(f"Line #{idx} (idx: {line_idx}): translated and stored\n")

  print(translation)

sys.stdout.flush()
sys.stderr.flush()
tmp_file_fd.close()

if len(translations) != current_translation:
  sys.stderr.write(f"WARNING: unexpected number of translations: got {len(translations)} but {current_translation} was expected\n")

if debug:
  sys.stderr.write("Translation finished\n")
