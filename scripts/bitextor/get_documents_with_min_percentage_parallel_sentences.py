
# We assume that we use: ??-??.documents.gz and ??-??.sent.gz from the same bitextor execution

import sys
import gzip
import base64

sentgz_file = sys.argv[1]
documentsgz_file = sys.argv[2]
min_percentage = float(sys.argv[3]) # Range in [0, 1]

if min_percentage < 0.0 or min_percentage > 1.0:
    raise Exception("Supported range is [0, 1]")

if not sentgz_file.endswith(".sent.gz") or not documentsgz_file.endswith(".documents.gz"):
    raise Exception("Unexpected input files")

def tokenize(s):
    return s.strip().split() # Simple word tokenizer

sentences_tokens = {}
documents_tokens = {}

with gzip.open(sentgz_file, "rt") as fd:
    next(fd) # skip header

    for l in fd:
        l = l.rstrip("\r\n").split('\t')
        urls_pair = f"{l[0]}\t{l[1]}"
        src = l[2].rstrip("\r\n")
        trg = l[3].rstrip("\r\n")
        sentences_tokens[urls_pair] = len(tokenize(src)) + len(tokenize(trg)) # Cannot be 0

total_docs = 0
printed_docs = 0

# Filter documents
with gzip.open(documentsgz_file, "rt") as fd:
    sys.stdout.write(next(fd))

    for idx, l in enumerate(fd, 1):
        l = l.split('\t') # We do not apply l.rstrip() because we want to print the output as it was received
        urls_pair = f"{l[0]}\t{l[1]}"
        src = base64.b64decode(l[2].rstrip("\r\n").encode()).decode("utf-8", errors="backslashreplace")
        trg = base64.b64decode(l[3].rstrip("\r\n").encode()).decode("utf-8", errors="backslashreplace")

        if urls_pair in documents_tokens:
            sys.stderr.write(f"WARNING: same URLs pair found more than once? Doc #{idx}: {urls_pair} {documents_tokens}\n")

        documents_tokens[urls_pair] = len(tokenize(src)) + len(tokenize(trg))

        if urls_pair not in sentences_tokens:
            sys.stderr.write(f"WARNING: URLs pair from documents.gz not found in sent.gz: doc #{idx}\n")
        else:
            if documents_tokens[urls_pair] < sentences_tokens[urls_pair]:
                sys.stderr.write("WARNING: more tokens in sent.gz than in documents.gz: this is expected "
                                 f"if bifixer was enabled: doc #{idx}: {documents_tokens[urls_pair]} "
                                 f" vs {sentences_tokens[urls_pair]}\n")

            percentage = min(sentences_tokens[urls_pair] / documents_tokens[urls_pair], 1.0)

            if percentage >= min_percentage:
                sys.stdout.write('\t'.join(l))

                printed_docs += 1

        total_docs += 1

if set(sentences_tokens.keys()) != set(documents_tokens.keys()):
    sys.stderr.write(f"WARNING: unexpected different URLs found in the provided files\n")

sys.stderr.write(f"INFO: {printed_docs} documents were printed of {total_docs} ({printed_docs * 100.0 / total_docs:.2f}%)\n")
