from utils import *

data = read_pickle('StreamingDataLog/twitterStream 20191106162133.pkl')

print(data[0])
print('\n')
print(data[1])

print(strip_tweet_text(data[0], retweets=False))
print(strip_tweet_text(data[1], retweets=False))
corpus = [strip_tweet_text(_dict, retweets=False) for _dict in data if strip_tweet_text(_dict,False) is not None]
tokenizer = Tokenizer(corpus=corpus)

results = tokenizer.clean_text()

print('Results: \n', results)
tokenizer.tokenize(keep_punc=False, remove_stop=True, lemmatize=True)

tokens = tokenizer.get_tokens()

print(tokens)

tokenizer.pad()

sequences = tokenizer.get_sequences()

print(sequences)