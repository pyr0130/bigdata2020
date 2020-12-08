import sys
import re
from operator import add

from pyspark import SparkContext

def map_phase(x):
    x = re.sub('--', ' ', x)
    x = re.sub("'", '', x)
    return re.sub('[?!@#$\'",.;:()]', '', x).lower()

def count_filter(line):
    global accum
    if line == 'Tokyo':
        accum += 1
     
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print >> sys.stderr, "Usage: wordcount <master> <inputfile> <outputfile>"
        exit(-1)

    sc = SparkContext(sys.argv[1], "python_wordcount_sorted in bigdataprogrammiing")
    lines = sc.textFile(sys.argv[2],2)

    accum = sc.accumulator(0)
    print(lines.getNumPartitions()) # print the number of partitions
    lines.foreach(count_filter)
    filtered_lines = lines.filter(lambda x:x!='Tokyo').map(map_phase)
    outRDD = filtered_lines.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1])
    #wordCounts = filtered_lines.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y)

    #outRDD = wordCounts.collect()
    outRDD.saveAsTextFile(sys.argv[3])
    print("Tokyo count: %d" % accum.value)
