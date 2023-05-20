git clone --depth 1 https://github.com/Angryrou/spark-sql-perf
cd spark-sql-perf
sbt package
cp target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar ../../resources
echo "done!"

cd ..
echo "Do you want to delete 'spark-sql-perf'? (Y/N)"
read response

if [[ "$response" == "Y" || "$response" == "y" ]]; then
    rm -rf spark-sql-perf
    echo "File 'spark-sql-perf' has been deleted."
else
    echo "File 'spark-sql-perf' was not deleted."
fi