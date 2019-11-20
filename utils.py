import pickle
import nltk
from nltk.stem import WordNetLemmatizer
import spacy
from spacy_langdetect import LanguageDetector
import tensorflow as tf
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.preprocessing.text import Tokenizer
import string
import re

def read_pickle(fp):
    tweets = list()
    with open(fp, 'rb') as f:
        while True:
            try:
                tweets.append(pickle.load(f))
            except EOFError:
                break
    return tweets

def remove_url(s):
    return re.sub(r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b', '', s, flags=re.MULTILINE)

def strip_tweet_text(tweet_dict, retweets=True, removeUrl=True):
    if retweets:
        if removeUrl:
            return tweet_dict['id'], remove_url(tweet_dict['text'])
        else:
            return tweet_dict['id'], tweet_dict['text']
    else:
        if 'retweeted_status' not in tweet_dict:
            if removeUrl:
                return tweet_dict['id'], remove_url(tweet_dict['text'])
            else:
                return tweet_dict['id'], tweet_dict['text']
        else:
            return None

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
        self.corpus = [{'id': _id, 'doc': doc} for _id, doc in corpus]
        self.nlp = spacy.load('en_core_web_sm')
        self.nlp.add_pipe(LanguageDetector(), name='language_detector', last=True)
        nltk.download('wordnet')
        self.lemmatizer = WordNetLemmatizer()
        self.table = str.maketrans("","", string.punctuation)

    def clean_text(self, keep_punc=False, remove_stop=True, lemmatize=True, **kwargs):
        """
        :param keep_punc: default False inidicating to not keep punctuation through pre-processing
        :param remove_stop: boolean indicating whether to remove stop words
        :param lemmatize: boolean indicating to lemmatize words
        :param kwargs: 'punc_list' - punctuation list of specific punctuations to remove.
        :return: updated corpus that has been pre-processed.
        """
        accepted_keys = ['punc_list']
        for key, val in kwargs.items():
            if key in accepted_keys:
                self.__dict__.update({key: val})
            else:
                raise ValueError('Unexpected kwarg found. Please use check which available kwargs are available.')
        self.__corpus = self.corpus
        for i, _dict in enumerate(self.corpus):
            corpus = {}
            corpus['id'] = _dict['id']
            # step 1, remove punctuation
            # step 2, lower case words
            if not keep_punc:
                corpus['doc'] = re.sub(r'[^\w\s]', '', _dict['doc'].translate(self.table).lower().strip(), flags=re.MULTILINE)
            else:
                corpus['doc'] = _dict['doc'].lower().strip()
            # step 3, remove stop words
            if remove_stop:
                corpus['doc'] = self.remove_stopwords(corpus['doc'])
            # step 4, lemmatize the words
            if lemmatize:
                corpus['doc'] = self.lemmatize(corpus['doc'])
            self.__corpus[i] = corpus
        return self.__corpus

    def remove_stopwords(self, doc):
        word_list = doc.split(' ')
        word_list = [word for word in word_list if not self.nlp.vocab[word].is_stop]
        return ' '.join(word_list)

    def lemmatize(self, doc):
        word_list = doc.split(' ')
        word_list = [self.lemmatizer.lemmatize(word) for word in word_list]
        return ' '.join(word_list)

class Tokenizer(WordCleaner):

    def __init__(self, corpus):
        super(Tokenizer, self).__init__(corpus)
        self.tokenizer = tf.keras.preprocessing.text.Tokenizer()
        self.__tokenization_results = list()

    def tokenize(self, keep_punc=False, remove_stop=True, lemmatize=True):
        self.__corpus = self.clean_text(keep_punc, remove_stop, lemmatize)
        docs = [(_dict['id'] ,_dict['doc']) for _dict in self.__corpus]
        full_doc = list()
        for _, doc in docs:
            full_doc.append(doc)
        corpus = ' '.join(full_doc)
        self.tokenizer.fit_on_texts([corpus])
        sequences = list()
        for _id, doc in docs:
            self.__tokenization_results.append({'id': _id, 'tokens': self.tokenizer.texts_to_sequences([doc])})
            sequences.append(self.tokenizer.texts_to_sequences(doc))
        self.max_sequence = len(max(sequences, key=len))

    def pad(self, length=None, pad_method='post'):
        if length is None:
            length = self.max_sequence
        self.__padded_sequences = list()
        for results in self.__tokenization_results:
            seq = results['tokens']
            id = results['id']
            pad = pad_sequences(seq, maxlen= length, padding = pad_method)

            self.__padded_sequences.append({'id': id, 'padded_tokens': pad})

    def get_tokens(self):
        return self.__tokenization_results

    def get_sequences(self):
        return self.__padded_sequences

    def get_message(self, _id):
        for _dict in self.__corpus:
            if _id in _dict:
                return _dict
        return None


