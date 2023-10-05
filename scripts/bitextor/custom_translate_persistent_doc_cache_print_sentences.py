
import sys
import base64
import hashlib
import subprocess

from pytools import persistent_dict

# This script gathers translations from disk-based storage which should be stored in other script, and print all the translations in order
# Expected to be used in the translation script in bitextor: python3 this_script.py <storage> <tmp_file> translation_script.sh <arg1> ... | ...
# For some reason, different results are obtained even when CPU-only is executed: https://github.com/browsermt/marian-dev/issues/104

debug = False
#debug = True

id_storage = base64.b64encode(sys.argv[1].encode()).decode() # Optional separate or common storage
tmp_file = sys.argv[2]

commands = sys.argv[3:]

storage = persistent_dict.PersistentDict(id_storage)
tmp_file_fd = open(tmp_file, "w")

for idx, doc in enumerate(sys.stdin, 1):
  doc = doc.rstrip("\r\n ")
#  doc += '\n'
  encoded_doc = doc.encode()
  md5_value = hashlib.md5(encoded_doc).hexdigest()

  try:
    # Try fetch the translation
    translation = storage.fetch(md5_value).rstrip("\r\n ")

    tmp_file_fd.write(f"1\t{idx}\t{translation}\n")

    if debug:
      sys.stderr.write(f"Doc #{idx}: cached\n")
  except persistent_dict.NoSuchEntryError:
    # Text has been not translated yet: translate

    tmp_file_fd.write(f"0\t{idx}\t{md5_value}\n")

    if debug:
      sys.stderr.write(f"Doc #{idx}: translation queued\n")

    print(doc)

tmp_file_fd.close()

print("CONTROL LINE: 42") # Last line to be sure that the tmp file is closed

if debug:
  sys.stderr.write("Preprocess finished\n")
