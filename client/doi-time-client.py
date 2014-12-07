# -*- coding: utf-8 -*-
#!/usr/bin/env python
### Example code to query CrossRef's api-time API
### The API details can be found: https://github.com/CrossRef/doi-time
### Author: Juan Pablo Alperin

import csv 
import requests
from StringIO import StringIO
from optparse import OptionParser

API_URL = 'http://148.251.184.90:3000/articles/'


parser = OptionParser()
parser.add_option("-f", "--infile", dest="infile",
                  help="CSV file with DOIs in it", metavar="FILE")
parser.add_option("-o", "--outfile", dest="outfile",
                  help="CSV file to be written with added columns", default="out.csv", metavar="FILE")
parser.add_option("-c", "--column", dest="doi_column", default='doi',
                  help="name of the column in the CSV file where the DOI is stored. If none supplied assumed to be no header and using first column")
parser.add_option("-n", "--batchsize", dest="batchsize", default=100,
                  help="How many dois to query in each request to doi-time")


(options, args) = parser.parse_args()

try:
	infile = open(options.infile)
except: 
    print 'No input file was provided, using a sample set of DOIs'
    exit(1)

csvReader = csv.reader(infile)

if options.doi_column is not None:
	header_row = csvReader.next()
	doi_column_index = header_row.index(options.doi_column)

output = {}
batch = []
all_dois = []
eof = False
resultsFound = False

while not eof:
	# read in the lines
	try:
		line = csvReader.next()
	except StopIteration:
		eof = True

	# build the batch of size "batchsize" (or to eof)
	if len(batch) < options.batchsize and not eof:
		doi = line[doi_column_index]  # grab a doi
		output[doi] = line        # save the line for writing back out
		all_dois.append(doi)	  # save the dois to output in original order
		batch.append(doi)         # add the doi to the batch being queried

	# its time to query a batch
	elif len(batch) > 0: 
		data = {'upload': '\n'.join(batch)}
		headers = { 'Accept': 'text/csv' } 

		# do the actual query
		r = requests.post(API_URL, data=data, headers=headers)
		r.raise_for_status()   # raise error if there was one

		# create a CSV reader with the response text
		reader = csv.DictReader(StringIO(r.text))

		for row in reader:
			resultsFound = True
			response_header_row = sorted([x for x in row.keys() if x != 'doi'])
		  	output[row['doi']] += [row[k] for k in response_header_row] 

		# clear the batch to start again
		batch = []
			

if not resultsFound:
	print "No results found for any of the given DOIs. No output file written."
	exit(1)

with open(options.outfile, 'wb') as outfile:
	output_header_row = header_row + ['cr_' + x for x in response_header_row]

	csvWriter = csv.writer(outfile)
	csvWriter.writerow(output_header_row)
	for doi in all_dois:
		row = output[doi]
		csvWriter.writerow(row + [''] * (len(output_header_row)-len(row)))

