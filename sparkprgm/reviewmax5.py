'''from pyspark import SparkContext
import sys

#filename=sys.argv[1]
def main():
	sc=SparkContext(appName='SparkWordCount')
	input_file=sc.textFile('/home/parth/Desktop/Hadoop_Files_py/Musical_instruments_reviews.csv')
	records = input_file.map(parserecord).filter(lambda rec:(rec[1]=='5'))
	mapvalues = records.map(lambda rec:((rec[0]),1))
	reduceout = mapvalues.reduceByKey(lambda a,b: a+b)
	reduceout.saveAsTextFile('/home/parth/Desktop/spark3.txt')	
	sc.stop()

def parserecord(line):
	fields = line.split(",")
	r_id = (fields[0])
	review = (fields[6])
	return(r_id,review)

if __name__=='__main__':
	main()'''
from pyspark import SparkContext
import sys

def main():
	sc = SparkContext(appName='SparkWordCount')
	input_file = sc.textFile('/home/parth/Desktop/Hadoop_Files_py/Musical_instruments_reviews.csv')
	counts = input_file.map(lambda line:line.split(',')[0]).map(lambda word:(word,1)).reduceByKey(lambda a,b:a+b)
	def rating(x):
		l = []
		if x[1]==5:
			l.append((x[0],x[1]))
			return l
		else:
			return []
	counts = counts.flatMap(lambda x : rating(x))
	counts.saveAsTextFile('/home/parth/Desktop/output2')
	sc.stop()

if __name__ == '__main__':
	main()



#min ax_value = df.agg({"any-column": "max"}).collect()[0][0]
#val dsTemp = ds.filter(d => d.temp > 25).map(d => (d.temp, d.device_name, d.cca3)