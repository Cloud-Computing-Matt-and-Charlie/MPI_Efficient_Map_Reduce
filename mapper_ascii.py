#!/usr/bin/env python
import sys
import string
# input comes from STDIN (standard input)
def read_input(file):
	for line in file:
		yield line.split()

def main(seperator='\t'):
	data = read_input(sys.stdin)
 	for words in data:
		for word in words:
			for x in word:
				if (x not in string.punctuation) and (ord(x) <= 122) and (ord(x) >= 33)
					print '%s\t%s' % (x.lower(), 1)				

if __name__ == "__main__":
	main()