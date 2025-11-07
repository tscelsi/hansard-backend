import spacy
from spacy.lang.en.stop_words import STOP_WORDS


def preprocess(texts: list[str]):
    """This function takes the spaCy documents found in this classes docs
        attribute and preprocesses them.

    The preprocessing pipeline tokenises each document and removes:
    1. punctuation
    2. spaces
    3. numbers
    4. urls
    5. stop words and single character words.

    It then lemmatises and lowercases each token and joins multi-word tokens
        together with an _.
    It then adds ngrams from a ngram list by joining matched ngrams in the
        corpus with an _.

    Args:
        ngrams (bool, optional): Whether to add ngrams or to keep the corpus
            as unigram. Defaults to True.
    """
    paras_processed: list[list[list[str]]] = []
    print(f"Preprocessing {len(texts)} texts...")
    nlp = spacy.load("en_core_web_sm", disable=["ner"])
    for doc in nlp.pipe(texts):
        sents: list[list[str]] = []
        for s in doc.sents:
            words: list[str] = []
            for w in s:
                # PREPROCESS: lemmatize
                # PREPROCESS: remove * puncuation
                #                    * words that are / contain numbers
                #                    * URLs
                #                    * stopwords
                #                    * words of length==1
                if (
                    not w.is_punct
                    and not w.is_space
                    and not w.like_num
                    and not any(i.isdigit() for i in w.lemma_)
                    and not w.like_url
                    and w.text.lower() not in STOP_WORDS
                    and len(w.lemma_) > 1
                ):
                    words.append(w.lemma_.lower().replace(" ", "_"))
            sents.append(words)
        paras_processed.append(sents)
        print(
            f"Processed {len(paras_processed)}/{len(texts)} texts.", end="\r"
        )
    return paras_processed
