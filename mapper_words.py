#!/usr/bin/env python
import sys
import string
# input comes from STDIN (standard input)
def read_input(file):
	for line in file:
		yield line.split()

def main(seperator='\t'):
	skip_flag = 0
	data = read_input(sys.stdin)
 	for words in data:
		for word in words:
			for x in word:
				if (ord(x) > 122) or (ord(x) < 33):
					skip_flag = 1
					break
			if skip_flag:
				skip_flag = 0
				continue
			else:
				print '%s\t%s' % (word.translate(None, string.punctuation).lower(), 1)

if __name__ == "__main__":
	main()