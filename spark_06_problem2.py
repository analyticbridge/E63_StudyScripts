# HW6 - Problem #2
#Define 2 RDDs and read text from HDFS
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext, DataFrame

conf = SparkConf().setMaster("local").setAppName("hw6prb2")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#Read emps.txt from HDFS
emps = sc.textFile("hdfs://localhost:8020/user/cloudera/hw6/emps.txt")

#split sentences in to words file retaining the structure of lines - use map instead of flatmap
emps_fields = emps.map(lambda x: x.split(","))
num_lines = emps_fields.count()
#emps_fields.collect()

#create employees Row object
# map each element to the corresponding column name i.e name, age and salary
employees = emps_fields.map(lambda e: Row(name = e[0], age = int(e[1]), salary = float(e[2])))
exmployees_s = employees.collect()

#Create DataFrame using the employees RDD
emp_df = sqlContext.createDataFrame(employees)
emp_df_s = emp_df.collect()

#Register the dataframe as a table
emp_df.registerTempTable("employees_table")

#Query the temp table to find employees who have a salary greater than 3500
high_salary = sqlContext.sql("select * from employees_table where salary>3500").collect()


print "*********************************************************************************"
print "Output from reading the emps.txt in to an RDD and converting to a DataFrame:\n", emp_df_s
print "Employees who have a salary greater than 3500: \n", high_salary
print "*********************************************************************************"
