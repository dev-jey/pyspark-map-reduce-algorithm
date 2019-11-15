from pyspark import SparkContext
from pyspark import SparkConf

from operator import add

conf = SparkConf().setAppName("Find maximum and minimum occurring values in pyspark")
sc = SparkContext(conf=conf)

# Read file into RDD
lines = sc.textFile("dataset.txt")

# Call collect() to get all data
llist = lines.collect()

digit_to_count = {}

#Getloop through the digits, create a list and get the count of each digit
for i in llist:
    data = sc.parallelize(list(i))
    counts = data.map(lambda x:
                      (x, 1)).reduceByKey(add).sortBy(lambda x: x[1],
                                                      ascending=False).collect()

    for (word, count) in counts:
        digit_to_count[word] = count
        print("{}: {}".format(word, count))

'''Calculation of the mean of all the values counted in the reducer'''
count = 0
_sum = 0
for key in digit_to_count:
    count += 1
    _sum += digit_to_count[key]


# maximum digit value
print('Maximum digit value ', {max(item for item in digit_to_count.values())})

# minimum digit value
print('Minimum digit value ', {min(item for item in digit_to_count.values())})

# if including the whitespaces and period in the data
print('The mean is: ', (_sum/count))
# if not in cluding the period and whitespaces in the data
print('The mean is: ', (_sum-100)/(count-2))
