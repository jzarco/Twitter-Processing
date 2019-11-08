import pickle
import nltk
from nltk.stem import WordNetLemmatizer
import spacy
from spacy_langdetect import LanguageDetector
import tensorflow as tf
from tensorflow.keras.preprocessing.sequence import pad_sequences

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
        self.corpus = [{'id': i, 'doc': doc} for i, doc in enumerate(corpus)]
        self.nlp = spacy.load('en_core_web_sm')
        self.nlp.add_pipe(LanguageDetector(),name='language_detector', last=True)
        nltk.download('wordnet')
        self.lemmatizer = WordNetLemmatizer()

    #def detect_languages(self, keep_lang='en'):
    #    for _dict in self.corpus:
    #        lan = self.nlp(_dict['doc'])._.language['language']
    #        if
    #        _dict['lan'] =

    def clean_text(self):
        #step 1, remove punctuation
        #step 2, lower case words
        #step 3, remove stop words
        #step 4, lemmatize the words
        #step 5, join the words back into string
        pass

    def remove_stopwords(self):
        pass

    def lemmatize(self):
        pass

class Tokenizer(WordCleaner):

    def __init__(self):
        super().__init__()
        self.tokenizer = tf.keras.preprocessing.text.Tokenizer(char_level = False)
        self.__tokenization_results = list()

    def tokenize(self):
        docs = [(_dict['id'] ,_dict['doc']) for _dict in self.corpus]
        sequences = list()
        for id,doc in docs:
            self.tokenizer.fit_on_texts(doc)
            self.__tokenization_results.append({'id': id, 'tokens':self.tokenizer.texts_to_sequences(doc)})
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


