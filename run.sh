sudo -u hdfs hadoop jar \
  /home/mltest/csvtoxml-0.0.2.jar \
  com.marklogic.hadoop.csvtoxml.CsvToXML \
  marklogic-textin-docout-2.xml /user/hdfs/npi/npidata_20150323-20150329.csv
