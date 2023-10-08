
# We assume that we use: ??-??.documents.gz and ??-??.sent.gz from the same bitextor execution

import sys
import gzip
import glob
import base64

sentgz_file = sys.argv[1] # "permanent" directory
documentsgz_file = sys.argv[2] # "permanent" directory
# Files necessary to count the sentences of the documents found in sent.gz:
## This might be replaced with execution of the same sentence spliter applied in bitextor
urlgz_files_src_lang_glob = sys.argv[3] # "data" directory
urlgz_files_trg_lang_glob = sys.argv[4] # "data" directory
sentencesgz_files_src_lang_glob = sys.argv[5] # "data" directory
sentencesgz_files_trg_lang_glob = sys.argv[6] # "data" directory

if len(sys.argv) != 7:
    raise Exception(f"7 args were expected, got {len(sys.argv)}")

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
documents_sentences_count_single_url = {"src": {}, "trg": {}}
documents_sentences_count = {}

# Read url.gz and sentences.gz
urlgz_files_src = glob.glob(urlgz_files_src_lang_glob)
urlgz_files_trg = glob.glob(urlgz_files_trg_lang_glob)
sentencesgz_files_src = glob.glob(sentencesgz_files_src_lang_glob)
sentencesgz_files_trg = glob.glob(sentencesgz_files_trg_lang_glob)

if len(urlgz_files_src) != len(sentencesgz_files_src):
    raise Exception(f"Different length in src url.gz and sentences.gz files: {len(urlgz_files_src)} vs {len(sentencesgz_files_src)}")
if len(urlgz_files_trg) != len(sentencesgz_files_trg):
    raise Exception(f"Different length in trg url.gz and sentences.gz files: {len(urlgz_files_trg)} vs {len(sentencesgz_files_trg)}")

# Read url.gz and sentences.gz
for urlgz_files, sentencesgz_files, direction in ((urlgz_files_src, sentencesgz_files_src, "src"), (urlgz_files_trg, sentencesgz_files_trg, "trg")):
    for file_idx, (urlgz_file, sentencesgz_file) in enumerate(zip(urlgz_files, sentencesgz_files), 1):
        urlgz_file_prefix = '/'.join(urlgz_file.split('/')[:-1])
        sentencesgz_file_prefix = '/'.join(sentencesgz_file.split('/')[:-1])

        if urlgz_file_prefix != sentencesgz_file_prefix:
            sys.stderr.write(f"WARNING: sent.gz and sentences.gz should have the same prefix path: {urlgz_file_prefix} vs {sentencesgz_file_prefix}\n")

        with gzip.open(urlgz_file, "rt") as fd_urlgz, gzip.open(sentencesgz_file, "rt") as fd_sentencesgz:
            for doc_idx, (l_urlgz, l_sentencesgz) in enumerate(zip(fd_urlgz, fd_sentencesgz), 1):
                l_urlgz = l_urlgz.rstrip("\r\n ")
                l_sentencesgz = base64.b64decode(l_sentencesgz.rstrip("\r\n ").encode()).decode("utf-8", errors="backslashreplace").rstrip("\r\n ")

                if l_urlgz in documents_sentences_count_single_url[direction]:
                    sys.stderr.write(f"WARNING: URL #{doc_idx} seen more than once in {urlgz_file} ({direction}): {l_urlgz}\n")

                documents_sentences_count_single_url[direction][l_urlgz] = l_sentencesgz.count('\n') + 1

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
            sentences_count[urls_pair]["src"] += l[index_to_check_if_bifixer_joined_sentences["src"]].count('+') + 1
            sentences_count[urls_pair]["trg"] += l[index_to_check_if_bifixer_joined_sentences["trg"]].count('+') + 1
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

        if src_url not in documents_sentences_count_single_url["src"]:
            sys.stderr.write(f"WARNING: src URL not found in url.gz: {src_url}\n")
        else:
            documents_sentences_count[urls_pair]["src"] = documents_sentences_count_single_url["src"][src_url]

        if trg_url not in documents_sentences_count_single_url["trg"]:
            sys.stderr.write(f"WARNING: trg URL not found in url.gz: {trg_url}\n")
        else:
            documents_sentences_count[urls_pair]["trg"] = documents_sentences_count_single_url["trg"][trg_url]

        documents_tokens[urls_pair] = {"src": 0, "trg": 0}
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
            if total_documents_sentences < total_sentences_sentences:
                sys.stderr.write("ERROR: more sentences in sent.gz than in sentences.gz files: "
                                 f"this is unexpected: doc #{idx}: {total_documents_tokens} "
                                 f" vs {total_sentences_sentences}\n")

            percentage_tokens = total_sentences_tokens / total_documents_tokens if total_documents_tokens > 0 else -1.0
            normalized_percentage_tokens = min(percentage_tokens, 1.0)
            percentage_sentences = total_sentences_sentences / total_documents_sentences if total_documents_sentences > 0 else -1.0
            normalized_percentage_sentences = min(percentage_sentences, 1.0)

            if normalized_percentage_tokens < 0.0:
                sys.stderr.write(f"WARNING: tokens percentage is negative: doc #{idx}: {normalized_percentage_tokens}: did you provide "
                                 "the arguments correctly?\n")
            if normalized_percentage_sentences < 0.0:
                sys.stderr.write(f"WARNING: sentences percentage is negative: doc #{idx}: {normalized_percentage_sentences}: did you provide "
                                 "the arguments correctly?\n")

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
