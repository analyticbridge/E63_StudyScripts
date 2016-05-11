#HW12 solution 02 code 
import nltk 
from nltk.book  import *

#tokenize and get the Data 

Data=[w.lower() for w in text4]
len(Data)

LongW=[w for w in Data if len(w) > 7]
len(LongW)

#get the frequency distribution 
#get most common(top)words 

fdist=FreqDist(LongW)
fdist.most_common(10)
Top10=fdist.most_common(10)
print(Top10)


# check the distribution Graph and plot


lengths = [len(w) for w in LongW]
fdlen=nltk.FreqDist(lengths)
fdlen.tabulate()
fdlen.plot(10,cumulative=False)
fdlen.tabulate()


# Synonyms using words net for top 10 word 


from nltk.corpus import wordnet as wn


Top10W= [('government'), ('citizens'),('constitution'), ('national'), ('american'), ('congress'), ('interests'), ('political'), ('executive'), ('principles')]

for ss in Top10W:
    print (ss + ':', wn.synsets(ss))
	

# test one for Hyponyms 

g=wn.synset('government.n.01')
g.hyponyms()

# find the Hyponyms for whole set 


Top10WH= [('government.n.01'), ('citizens.n.01'),('constitution.n.01'), ('national.n.01'), ('american.n.01'), ('congress.n.01'), ('interests.n.01'), ('political.n.01'), ('executive.n.01'), ('principles.n.01')]

#defind function and apply to word list 


def get_hyponyms(synset):
    hyponyms = set()
    for hyponym in synset.hyponyms():
        hyponyms = set(get_hyponyms(hyponym))
    return hyponyms

for ss in Top10WH:
    print (ss + ':', set(synset.hyponyms()))
	
##end of code S02 

	