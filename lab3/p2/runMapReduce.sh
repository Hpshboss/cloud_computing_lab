echo "\n**************************************************************************************************************"
echo "Refresh hbase.txt"
echo "**************************************************************************************************************"

hadoop fs -rm -r /user/<studentID>/hbase.txt
hadoop fs -put hbase.txt /user/<studentID>/hbase.txt


echo "\n**************************************************************************************************************"
echo "Compile And Run hbaseMapredInput"
echo "**************************************************************************************************************"

echo "\n*************************javac -cp `hbase classpath` hbaseMapredInput.java************************************"
javac -cp `hbase classpath` hbaseMapredInput.java
echo "\n************************jar cf hbaseMapredInput.jar hbaseMapredInput*.class***********************************"
jar cf hbaseMapredInput.jar hbaseMapredInput*.class
echo "\n****************hadoop jar hbaseMapredInput.jar hbaseMapredInput hbase.txt s109323085*************************"
hadoop jar hbaseMapredInput.jar hbaseMapredInput hbase.txt <studentID>


echo "\n**************************************************************************************************************"
echo "Remove Output Of Mapreduce In Advance"
echo "**************************************************************************************************************"

hadoop fs -rm -r /user/<studentID>/<outputDir>/part-r-00000
hadoop fs -rm -r /user/<studentID>/<outputDir>/_SUCCESS
hadoop fs -rmdir /user/<studentID>/<outputDir>


echo "\n**************************************************************************************************************"
echo "Compile And Run hbaseMapredOutput"
echo "**************************************************************************************************************"

echo "\n*************************javac -cp `hbase classpath` hbaseMapredOutput.java***********************************"
javac -cp `hbase classpath` hbaseMapredOutput.java
echo "\n***********************jar cf hbaseMapredOutput.jar hbaseMapredOutput*.class**********************************"
jar cf hbaseMapredOutput.jar hbaseMapredOutput*.class
echo "\n****************hadoop jar hbaseMapredInput.jar hbaseMapredInput hbase.txt s109323085*************************"
hadoop jar hbaseMapredOutput.jar hbaseMapredOutput <outputDir> <studentID>


echo "\n**************************************************************************************************************"
echo "Cat Result"
echo "**************************************************************************************************************"
hadoop fs -cat /user/<studentID>/<outputDir>/part-r-00000
