from pyspark import SparkContext
import sys
import csv
import re

def main():
	search = sys.argv[1]
	sc = SparkContext(appName='SparkWordCount')
	input_file = sc.textFile("/home/parth/Desktop/Hadoop_Files_py/movies.csv")
	counts = input_file.map(parse).filter(lambda x: re.search(r'\b{}\b'.format(search),x[1]))
	mapvalues = counts.map(lambda x:((x[1]),x[2]))
	reduceout = mapvalues.reduceByKey(lambda a,b :a+b)
	counts.saveAsTextFile('/home/parth/Desktop/output3')
	sc.stop()

def parse(line):
	fields = line.split(",")
	mid = (fields[0])
	mtitle = (fields[1])
	mgenre = (fields[2])
	return(mid,mtitle,mgenre)	

if __name__=='__main__':
	main()
