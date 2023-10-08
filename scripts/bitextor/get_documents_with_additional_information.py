
# We assume that we use: ??-??.documents.gz and ??-??.sent.gz from the same bitextor execution

import sys
import gzip
import glob
import base64

sentgz_file = sys.argv[1] # "permanent" directory
documentsgz_file = sys.argv[2] # "permanent" directory
urlgz_files_glob = sys.argv[3] # "data" directory
sentencesgz_files_glob = sys.argv[4] # "data" directory

if not sentgz_file.endswith(".sent.gz") or not documentsgz_file.endswith(".documents.gz"):
    raise Exception("Unexpected input files")

def tokenize(s):
    return s.strip().split() # Simple word tokenizer

def check_it_was_last_line_in_fd(fd):
    try:
        next(fd)

        return False
    except StopIteration:
        return True

sentences_tokens = {}
sentences_count = {}
documents_tokens = {}
documents_sentences_count_single_url = {}
documents_sentences_count = {}

# Read url.gz and sentences.gz
urlgz_files = glob.glob(urlgz_files_glob)
sentencesgz_files = glob.glob(sentencesgz_files_glob)

if len(urlgz_files) != len(sentencesgz_files):
    raise Exception(f"Different length in url.gz and sentences.gz files: {len(urlgz_files)} vs {len(sentencesgz_files)}")

# Read url.gz and sentences.gz
for file_idx, (urlgz_file, sentencesgz_file) in enumerate(zip(urlgz_files, sentencesgz_files), 1):
    with gzip.open(urlgz_file, "rt") as fd_urlgz, gzip.open(sentencesgz_file, "rt") as fd_sentencesgz:
        for doc_idx, (l_urlgz, l_sentencesgz) in enumerate(zip(fd_urlgz, fd_sentencesgz), 1):
            l_urlgz = l_urlgz.rstrip("\r\n ")
            l_sentencesgz = base64.b64decode(l_sentencesgz.rstrip("\r\n ").encode()).decode("utf-8", errors="backslashreplace").rstrip("\r\n ")

            if l_urlgz in documents_sentences_count_single_url:
                sys.stderr.write(f"WARNING: URL #{doc_idx} seen more than once in {urlgz_file}: {l_urlgz}\n")

            documents_sentences_count_single_url[l_urlgz] = l_sentencesgz.count('\n') + 1

        if not check_it_was_last_line_in_fd(fd_urlgz) or not check_it_was_last_line_in_fd(fd_sentencesgz):
            raise Exception(f"Reading url.gz and sentences.gz #{file_idx}: {urlgz_file} and {sentencesgz_file}: different length")

index_to_check_if_bifixer_joined_sentences = None

# Read sent.gz
with gzip.open(sentgz_file, "rt") as fd:
    header = next(fd).rstrip("\r\n ").split('\t')

    if "src_deferred_hash" in header and "trg_deferred_hash" in header:
        src_field = header.index("src_deferred_hash")
        trg_field = header.index("trg_deferred_hash")
        index_to_check_if_bifixer_joined_sentences = {"src": src_field, "trg": trg_field}
    elif "src_paragraph_id" in header and "trg_paragraph_id" in header:
        src_field = header.index("src_paragraph_id")
        trg_field = header.index("trg_paragraph_id")
        index_to_check_if_bifixer_joined_sentences = {"src": src_field, "trg": trg_field}

    for l in fd:
        l = l.rstrip("\r\n ").split('\t')
        src_url = l[0]
        trg_url = l[1]
        urls_pair = f"{src_url}\t{trg_url}"
        src = l[2].rstrip("\r\n ")
        trg = l[3].rstrip("\r\n ")

        if urls_pair not in sentences_tokens:
            sentences_tokens[urls_pair] = {"src": 0, "trg": 0}
            sentences_count[urls_pair] = {"src": 0, "trg": 0}

        sentences_tokens[urls_pair]["src"] += len(tokenize(src))
        sentences_tokens[urls_pair]["trg"] += len(tokenize(trg))

        if index_to_check_if_bifixer_joined_sentences:
            sentences_count[urls_pair]["src"] += l[index_to_check_if_bifixer_joined_sentences["src"]].count('+') - 1
            sentences_count[urls_pair]["trg"] += l[index_to_check_if_bifixer_joined_sentences["trg"]].count('+') - 1
        else:
            sentences_count[urls_pair]["src"] += 1
            sentences_count[urls_pair]["trg"] += 1

total_docs = 0
printed_docs = 0

# Filter documents
with gzip.open(documentsgz_file, "rt") as fd:
    header = next(fd).rstrip("\r\n ").split('\t')

    # Tokens
    header.append("src_sentences_tokens")
    header.append("trg_sentences_tokens")
    header.append("total_sentences_tokens")
    header.append("src_documents_tokens")
    header.append("trg_documents_tokens")
    header.append("total_documents_tokens")
    header.append("percentage_parallel_tokens")
    header.append("normalized_percentage_tokens")
    # Sentences
    header.append("src_sentences_sentences")
    header.append("trg_sentences_sentences")
    header.append("total_sentences_sentences")
    header.append("src_documents_sentences")
    header.append("trg_documents_sentences")
    header.append("total_documents_sentences")
    header.append("percentage_parallel_sentences")
    header.append("normalized_percentage_sentences")

    print('\t'.join(header))

    for idx, l in enumerate(fd, 1):
        l = l.rstrip("\r\n ").split('\t')
        src_url = l[0]
        trg_url = l[1]
        urls_pair = f"{src_url}\t{trg_url}"
        src = base64.b64decode(l[2].rstrip("\r\n ").encode()).decode("utf-8", errors="backslashreplace").rstrip("\r\n ")
        trg = base64.b64decode(l[3].rstrip("\r\n ").encode()).decode("utf-8", errors="backslashreplace").rstrip("\r\n ")

        if urls_pair in documents_tokens:
            sys.stderr.write(f"WARNING: same URLs pair found more than once? Doc #{idx}: {urls_pair} {documents_tokens}\n")

        documents_sentences_count[urls_pair] = {"src": 0, "trg": 0}

        if src_url not in documents_sentences_count_single_url:
            sys.srderr.write(f"WARNING: src URL not found in url.gz: {src_url}\n")
        else:
            documents_sentences_count[urls_pair]["src"] = documents_sentences_count_single_url[src_url]

        if trg_url not in documents_sentences_count_single_url:
            sys.srderr.write(f"WARNING: trg URL not found in url.gz: {trg_url}\n")
        else:
            documents_sentences_count[urls_pair]["trg"] = documents_sentences_count_single_url[trg_url]

        documents_tokens[urls_pair]["src"] = len(tokenize(src))
        documents_tokens[urls_pair]["trg"] = len(tokenize(trg))

        if urls_pair not in sentences_tokens or urls_pair not in sentences_count:
            sys.stderr.write(f"WARNING: URLs pair from documents.gz not found in sent.gz or (url.gz + sentences.gz): doc #{idx}\n")
        else:
            total_sentences_tokens = sentences_tokens[urls_pair]["src"] + sentences_tokens[urls_pair]["trg"]
            total_documents_tokens = documents_tokens[urls_pair]["src"] + documents_tokens[urls_pair]["trg"]
            total_sentences_sentences = sentences_count[urls_pair]["src"] + sentences_count[urls_pair]["trg"]
            total_documents_sentences = documents_sentences_count[urls_pair]["src"] + documents_sentences_count[urls_pair]["trg"]

            if total_documents_tokens < total_sentences_tokens:
                sys.stderr.write("WARNING: more tokens in sent.gz than in documents.gz: this is expected "
                                 f"if bifixer was enabled: doc #{idx}: {total_documents_tokens} "
                                 f" vs {total_sentences_tokens}\n")

            percentage_tokens = total_sentences_tokens / total_documents_tokens
            normalized_percentage_tokens = min(percentage_tokens, 1.0)
            percentage_sentences = total_sentences_sentences / total_documents_sentences
            normalized_percentage_sentences = min(percentage_sentences, 1.0)

            # Tokens
            l.append(str(sentences_tokens[urls_pair]["src"]))
            l.append(str(sentences_tokens[urls_pair]["trg"]))
            l.append(str(total_sentences_tokens))
            l.append(str(documents_tokens[urls_pair]["src"]))
            l.append(str(documents_tokens[urls_pair]["trg"]))
            l.append(str(total_documents_tokens))
            l.append(f"{percentage_tokens:.2f}")
            l.append(f"{normalized_percentage_tokens:.2f}")
            # Sentences
            l.append(str(sentences_count[urls_pair]["src"]))
            l.append(str(sentences_count[urls_pair]["trg"]))
            l.append(str(total_sentences_sentences))
            l.append(str(documents_sentences_count[urls_pair]["src"]))
            l.append(str(documents_sentences_count[urls_pair]["trg"]))
            l.append(str(total_documents_sentences))
            l.append(f"{percentage_sentences:.2f}")
            l.append(f"{normalized_percentage_sentences:.2f}")

            print('\t'.join(l))

            printed_docs += 1

        total_docs += 1

if set(sentences_tokens.keys()) != set(documents_tokens.keys()):
    sys.stderr.write(f"WARNING: unexpected different URLs found in the provided files\n")

percentage_printed_docs = (printed_docs * 100.0 / total_docs) if total_docs > 0 else 0.0

sys.stderr.write(f"INFO: {printed_docs} documents were printed of {total_docs} ({percentage_printed_docs:.2f}%)\n")
