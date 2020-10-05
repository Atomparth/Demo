from pyspark import SparkContext
import sys

#filename=sys.argv[1]
def main():
	sc=SparkContext(appName='SparkWordCount')
	input_file=sc.textFile('/home/parth/Desktop/input.txt')
	counts=input_file.flatMap(lambda line:line.split()).map(lambda word:(word,1)).reduceByKey(lambda a,b: a+b)
	counts.saveAsTextFile('/home/parth/Desktop/input2.txt')
	sc.stop()

if __name__=='__main__':
	main()