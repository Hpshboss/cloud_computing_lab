from pyspark import SparkContext, SparkConf

appName = "[app name]"
master = "yarn"

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

inputFilePath = "hdfs://hadoop-master:[port]/user/[student id]/practice4-1.txt"

text_file = sc.textFile(inputFilePath)

lines_with_shoe = text_file.filter(lambda line: "shoe" in line)

ansList = []
for i in range(len(lines_with_shoe.collect())):
    ansList.append(lines_with_shoe.collect()[i].encode('ascii', 'ignore'))

print("")
print("")
print("AnswerHere")
print("")

for i in range(len(ansList)):
    print(ansList[i])

print("")
