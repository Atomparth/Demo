from pyspark import SparkContext
import sys

#filename=sys.argv[1]
def main():
	sc=SparkContext(appName='SparkWordCount')
	input_file=sc.textFile('/home/parth/Desktop/Hadoop_Files_py/Musical_instruments_reviews.csv')
	reviewer=input_file.map(lambda line:line.split(',')[0])  # To take a particular column value # and return review 
	counts=reviewer.map(lambda word: (word ,1)).reduceByKey(lambda a,b:a+b)
	counts.saveAsTextFile('/home/parth/Desktop/spark1.txt')
	sc.stop()
if __name__=='__main__':
	main()




'''	CREATE TABLE CUSTOMERS(
  ID  INT            NOT NULL,
  NAME VARCHAR (20)     NOT NULL,
  AGE  INT            NOT NULL,
  ADDRESS  CHAR (25) ,
  SALARY   DECIMAL (18, 2),
  PRIMARY KEY (ID)
);






	CREATE TABLE ORDERS (
  ID         INT NOT NULL,
  DATE        DATETIME,
  CUSTOMER_ID INT references CUSTOMERS(ID),
  AMOUNT     double,
  PRIMARY KEY (ID)
);
create table Cart
(
crt_id int AUTO_INCREMENT,
p_id int,
c_id int,
all_qty int,
o_dlrystatus tinyint NOT NULL,
foreign key (p_id) references Product(p_id),
foreign key (c_id) references Customer(c_id),
primary key (crt_id)
);
'''