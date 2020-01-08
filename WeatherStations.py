#!/usr/bin/env python3
#!/usr/local/spark python3
#Faddy Sunna, CS236, Fall 2019

from __future__ import print_function
from pyspark.sql.functions import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys
import pyspark.sql.functions as func
import argparse
import time
from pyspark.sql.functions import count, avg
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


sc= SparkContext()
sqlContext = SQLContext(sc)
def run(args):
    
    #starts timer
    start_time = time.time()
    #import recordings the text files (recordings data) in the folder given as a spark dataframe
    recordings = sqlContext.read.load(args.recordings + '/*.txt', format="text", header = 'true')
    #select the STN, YEARMODA, and TEMP colunmns
    recordings = recordings.select(recordings.value.substr(1, 6).alias('STN'),recordings.value.substr(15, 8).alias('YEARMODA'), recordings.value.substr(27, 4).alias('TEMP'))
    #Filter out the extra headers
    recordings = recordings.filter((recordings.STN != 'STN') & (recordings.YEARMODA != 'YEARMODA') & (recordings.TEMP != 'TEMP'))

    #import recordings the csv files (weather station location) in the folder given as a spark dataframe
    stalocs = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(args.locations + '/*.csv')



    
    #Select only the month values in the YEARMODA column from format yyyymmdd
    recordings = recordings.withColumn('YEARMODA', recordings['YEARMODA'].substr(5, 2))
    #convenrt the numerical months into strings
    recordings = recordings.withColumn('YEARMODA', regexp_replace('YEARMODA', '01', 'January'))
    recordings = recordings.withColumn('YEARMODA', regexp_replace('YEARMODA', '02', 'February'))
    recordings = recordings.withColumn('YEARMODA', regexp_replace('YEARMODA', '03', 'March'))
    recordings = recordings.withColumn('YEARMODA', regexp_replace('YEARMODA', '04', 'April'))
    recordings = recordings.withColumn('YEARMODA', regexp_replace('YEARMODA', '05', 'May'))
    recordings = recordings.withColumn('YEARMODA', regexp_replace('YEARMODA', '06', 'June'))
    recordings = recordings.withColumn('YEARMODA', regexp_replace('YEARMODA', '07', 'July'))
    recordings = recordings.withColumn('YEARMODA', regexp_replace('YEARMODA', '08', 'August'))
    recordings = recordings.withColumn('YEARMODA', regexp_replace('YEARMODA', '09', 'September'))
    recordings = recordings.withColumn('YEARMODA', regexp_replace('YEARMODA', '10', 'October'))
    recordings = recordings.withColumn('YEARMODA', regexp_replace('YEARMODA', '11', 'November'))
    recordings = recordings.withColumn('YEARMODA', regexp_replace('YEARMODA', '12', 'December'))

    #Filter out country to US, and States/territories that are in the statelist
    stalocs = stalocs.where(stalocs.STATE.isNotNull())
    stalocs = stalocs.filter(stalocs['CTRY'] == 'US')
    statelist = ['AL', 'AK', 'AS', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FM', 'FL', 'GA', 'GU', 'HI',
                              'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MH', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO',
                              'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'MP', 'OH', 'OK', 'OR', 'PW', 'PA',
                              'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VI', 'VA', 'WA', 'WV', 'WI', 'WY', 'AE',
                              'AA', 'AP']

    #select only the USAF and STATE columns
    stalocs = stalocs.select("USAF", "STATE")
    stalocs = stalocs.filter(stalocs['STATE'].isin(statelist))

    #join the recordings from all the years wit the station locations based on USAF == STN
    join = recordings.join(stalocs, recordings['STN'] == stalocs['USAF'])
    #Group the joined df, and find the average temp for each state and month
    final = join.groupBy("STATE", "YEARMODA").agg(avg("TEMP"), count("*"))
    #round the average temp to 3 decimal places
    final = final.withColumn("avg(TEMP)", func.round(final["avg(TEMP)"], 3))

    #create window partition to partition by State, and order by month
    windowSpec = Window.partitionBy(final['STATE']).orderBy(final['YEARMODA'].desc()).rangeBetween(-sys.maxsize, sys.maxsize)
    # dataFrame = sqlContext.table("final")
    #function for calculating difference between max avg temp and min avg temp for each State
    temp_diff = (func.max(final['avg(TEMP)']).over(windowSpec) - func.min(final['avg(TEMP)']).over(windowSpec))
    #create new df with max average, min average, and temp difference
    finaly = final.select(
        final['STATE'],
        final['YEARMODA'],
        final['avg(TEMP)'],
        temp_diff.alias("td"), func.max(final['avg(TEMP)']).over(windowSpec), func.min(final['avg(TEMP)']).over(windowSpec))
    #rename min and max columns
    finaly = finaly.withColumnRenamed('max(avg(TEMP)) OVER (PARTITION BY STATE ORDER BY YEARMODA DESC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)', 'max')
    finaly = finaly.withColumnRenamed('min(avg(TEMP)) OVER (PARTITION BY STATE ORDER BY YEARMODA DESC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)', 'min')

    #Create new df with only the MAX average temps for each state its month. each state will appear only once
    finalymax = finaly.select(final['STATE'],finaly['YEARMODA'],finaly['avg(TEMP)'],finaly['td'],finaly['max'],finaly['min']).where((finaly['avg(TEMP)'] == finaly['max']))
    # Create new df with only the MIN average temps for each state its month. each state will appear only once
    finalymin = finaly.select(final['STATE'],finaly['YEARMODA'],finaly['avg(TEMP)'],finaly['td'],finaly['max'],finaly['min']).where((finaly['avg(TEMP)'] == finaly['min']))



    print('AVERAGE TEMP FOR EACH STATE AND MONTH')
    print('-----------------------------------------')
    final = final.sort("STATE", 'YEARMODA')
    final.show(636)

    #Create two new dataframes, min and max average temp for each state and its month
    finalymin = finalymin.withColumn("min", finalymax["min"].cast(StringType()))
    finalymax = finalymax.withColumn("max", finalymax["max"].cast(StringType()))

    #Print the maximum average temp dataframe as a list of sentences
    print("MAXIMUM AVG TEMP FOR EACH STATE AND ITS MONTH")
    print('-----------------------------------------')
    for f in finalymax.collect():
        print('The MAXIMUM average temp for ' + f['STATE'] + ' was ' + f['max'] + ' during the month of ' + f['YEARMODA'])
    print('-----------------------------------------')

    # Print the minimum average temp dataframe as a list of sentences
    print("MINIMUM AVG TEMP FOR EACH STATE AND ITS MONTH")
    print('-----------------------------------------')
    for f in finalymin.collect():
        print('The MINIMUM average temp for ' + f['STATE'] + ' was ' + f['min'] + ' during the month of ' + f['YEARMODA'])

    #Coalesce the partitions and write as a single .csv file. Done for three data frames, outputing three .csv files.
    final.coalesce(1).write.format('com.databricks.spark.csv').save(args.output + 'avgTemp_state.csv', header='true')
    finalymax.coalesce(1).write.format('com.databricks.spark.csv').save(args.output + 'Max_avgtemp.csv',header = 'true')
    finalymin.coalesce(1).write.format('com.databricks.spark.csv').save(args.output + 'Min_avgtemp.csv', header='true')

    #end timer, and print elapsed time
    end_time = time.time()
    total_time = end_time - start_time
    print("Execution time in seconds:  ", total_time)

#Use argparse to create the file location input and output commands
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # Required arguments.
    parser.add_argument(
        "-l",
        "--locations",
        required=True,
        help="Path to directory to locations")
    parser.add_argument(
        "-r",
        "--recordings",
        required=True,
        help="Path to directory containing text file recordings")
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="Path to directory of csv outputs")

    args = parser.parse_args()
    run(args)

