import pickle
import nltk
nltk.download('wordnet')
from nltk.stem import WordNetLemmatizer
import spacy
from spacy_langdetect import LanguageDetector

nlp = spacy.load('en')
lemmatizer = WordNetLemmatizer()

def read_pickle(fp):
    with open(fp, 'rb') as f:
        data = pickle.load(f)
    return data

def hashtags_counter(data):
    """
    :param data: data dictionary of Tweet JSON objects
    :return: returns the count of the specified Hashtag along with a list of postIds that incorporated the hashtag.
    """
    hashtags = {}
    textBlobs = [_dict['text'] for _dict in data]
    postIds = [_dict['id'] for _dict in data]

    for text, post in zip(textBlobs,postIds):
        tags = [tag.strip("#").lower() for tag in text.split() if tag.startswith('#')]
        for tag in tags:
            if tag in hashtags:
                hashtags[tag]['count'] += 1
                hashtags[tag]['postIds'].append(post)
            else:
                hashtags[tag]['count'] = 1
                hashtags[tag]['postIds'] = [post]
    return hashtags

class WordCleaner:

    def __init__(self, corpus):
        self.corpus = corpus

    def detect_languages(self, keep_lang='en'):
        pass

    def clean_text(self):
        pass

    def remove_stopwords(self):
        pass

    def lemmatize(self):
        pass

def clean_text(text):
    pass



