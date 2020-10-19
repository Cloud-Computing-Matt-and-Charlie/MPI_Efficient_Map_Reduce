#!/usr/bin/env python
from itertools import groupby
from operator import itemgetter 
import sys


#initialize most frequent and least frequent lists
max_print = 10
most_freq = [[0, ' '] for i in range(max_print)]
least_freq = [[sys.maxint,' '] for i in range(max_print)]

def read_mapper_output(file, seperator='\t'):
	for line in file:
		yield line.rstrip().split(seperator,1)

def main(seperator='\t'):
	data = read_mapper_output(sys.stdin, seperator = seperator)
	for current_word, group in groupby(data,itemgetter(0)):
		try:
			total_count = sum(int(count) for current_word, count in group)
			for count, item in enumerate(most_freq):
				if total_count == item[0]:
					item.append(current_word)
				elif total_count > item[0]:
					most_freq.pop()
					most_freq.insert(count,[total_count,current_word])
					break
			for count, item in enumerate(least_freq):
				if total_count == item[0]:
					item.append(current_word)
					break
				elif total_count < item[0]:
					least_freq.pop()
					least_freq.insert(count, [total_count,current_word])
					break
			print "%s%s%d" % (current_word,seperator,total_count)
		except ValueError:
			pass

if __name__ == "__main__":
	main()
	print("MOST FREQUENT WORDS (freq: word)")
	for item in most_freq:
		x = list(item[1:])
		print(str(item[0]) + ": ("+ str(len(x)) + " items)")
		print(x)

	print("LEAST FREQUENT WORDS (freq: words)")
	for item in least_freq:
		x = list(item[1:])
		print(str(item[0]) + ": ("+ str(len(x)) + " items)")
		print(x)
