#HW12 NLTL Solution 01 code 
import nltk 
from nltk.corpus import gutenberg
nltk.corpus.gutenberg.fileids()

#check for corpus files 
gutenberg.fileids()

#find total number of words in a text 

for fileid in gutenberg.fileids():
    text_words=len(gutenberg.words(fileid))
    print ((text_words),fileid)

#total model frequencies in corpus 

from nltk.corpus import gutenberg
news_text = gutenberg.words()
fdist = nltk.FreqDist([w.lower() for w in news_text])
modals = ['can', 'could', 'may', 'might', 'must', 'will']

for m in modals:
    print (m + ':', fdist[m])

#relative frequencies 

from nltk.corpus import gutenberg
from nltk.probability import ConditionalFreqDist
cfd = nltk.ConditionalFreqDist(
  (id, word)
  for id in gutenberg.fileids()
  for word in gutenberg.words(fileids=id))
id = gutenberg.fileids()
modals = ['can', 'could', 'may', 'might', 'must', 'will']
cfd.tabulate(conditions=id, samples=modals)


# Examin Concordances 

bible = nltk.Text(nltk.corpus.gutenberg.words('bible-kjv.txt'))

bible.concordance("will")
	
burgress = nltk.Text(nltk.corpus.gutenberg.words('burgess-busterbrown.txt'))

burgress.concordance("may")

#solution code ends 	