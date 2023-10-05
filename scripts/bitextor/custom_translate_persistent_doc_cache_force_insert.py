
import sys
import base64
import hashlib

from pytools import persistent_dict

# Example: echo -e "doc1_base64\tdoc1_translation_base64\ndoc2_base64\tdoc2_translation_base64" | python3 custom_translate_persistent_doc_cache_force_insert.py translation_is2en

id_storage = base64.b64encode(sys.argv[1].encode()).decode() # Optional separate or common storage

storage = persistent_dict.PersistentDict(id_storage)

for idx, doc in enumerate(sys.stdin, 1):
  src_doc, trg_doc = doc.split('\t')
  trg_doc = trg_doc.rstrip("\r\n ")
  src_doc = src_doc.rstrip("\r\n ")
#  src_doc += '\n'
#  trg_doc += '\n'
  encoded_src_doc = src_doc.encode()

  md5_value = hashlib.md5(encoded_src_doc).hexdigest()

  storage.store(md5_value, trg_doc)
