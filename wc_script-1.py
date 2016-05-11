#!/usr/bin/python

import sys
import csv

stop_words = ['his', 'he', 'i', 'was', 'with', 'for', 'her', 'the', 
			  'that', 'to', 'as', 'you', 'at', 'him', 'all', 'from',
			  'be', 'she', 'they', 'had', 'an', 'And', 'them', 'he' ,
			  'there', 'has', 'and', 'or', 'is', 'not', 'a', 
			  'of', 'but', 'in', 'by', 'on', 'are', 'it', 'if']

if __name__ == '__main__':

	wc = {}
	# read input file
	with open(sys.argv[1:][0], 'rb') as csvfile:
		fc = csv.reader(csvfile, delimiter='\t')
		# fill in dict (unsorted)
		for row in fc:
			# filter out stop words
			if (row[0].lower() in stop_words):
				continue
			wc[row[0]] = int(row[1])
	
	# sort the dict by vlues and pick the top 200 items only
	sorted_wc = sorted(wc.iteritems(), key=lambda x:x[1], reverse=True)[:200]
	
	# write the result to console as CSV
	for x in sorted_wc:
		print "%s\t%s" %(x[0],x[1])