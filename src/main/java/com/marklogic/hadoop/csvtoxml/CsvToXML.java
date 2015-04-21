/*
Copyright 2015 MarkLogic Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

@author Philip Pax https://www.linkedin.com/in/philippaz

*/
package com.marklogic.hadoop.csvtoxml;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.DocumentURI;

/**
 * Load documents from HDFS into MarkLogic DB.
 */
public class CsvToXML {

  static ArrayList<String> beginElementNameList = constructBeginList();
  static ArrayList<String> endElementNameList = constructEndList();
  static String noQuote = "\"";
  static String noNeed = "<UNAVAIL>";

  public static class ContentMapper extends Mapper<LongWritable, Text, DocumentURI, Text> {

    private DocumentURI uri = new DocumentURI();
    private String inputString;
    private int counter = 1;
    private Text fileContent;
    private String fileName;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // Could use OpenCSV, just another external library
      String TempString = value.toString();
      String[] SingleNPIDataRow = TempString.split("\",\"");

      // REMOVE LEADING Quote at the beginning of the CSV line
      if(SingleNPIDataRow[0].trim().startsWith(noQuote)) {
        SingleNPIDataRow[0] = SingleNPIDataRow[0].substring(1);
      }

      inputString = constructPropertyXml(SingleNPIDataRow);

      // If counter is 1, the csv_column_name_values_
      // Print all header name value as 0000000000.xml
      if (counter == 1){
        fileName = "/0000000000.xml";
        inputString = "";
        inputString  = "<EIPData><RECORDS><DATA><NPI>0000000000</NPI></DATA></RECORDS></EIPData>";
      } else {
        // Unique FileName, NPI value SingleNPIDataRow[0]
        fileName = SingleNPIDataRow[0]+".xml";
      }
      uri.setUri(fileName);

      fileContent = new Text(inputString);

      try {
        context.write(uri, fileContent);
      } catch (Exception e) {
        System.err.println("E to the E " + e);
      }

      counter = counter + 1;
      inputString = "";
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    @SuppressWarnings("deprecation")
    Job job = new Job(conf);
    job.setJobName("ProcessCSVtoXML_job");
    System.out.println("After the JobName Updates");
    job.setJarByClass(CsvToXML.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(ContentMapper.class);
    job.setMapOutputKeyClass(DocumentURI.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputFormatClass(ContentOutputFormat.class);
    System.out.println("Made it past external jar dependencies nodes");

    FileInputFormat.setInputPaths(job, new Path(otherArgs[1]));

    conf = job.getConfiguration();
    conf.addResource(otherArgs[0]);
    System.out.println("After the conf.set");

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static String constructPropertyXml(String[] SingleNPIDataRow) {

    //REMOVE LEADING Quote at the beginning of the CSV line
    //if(SingleNPIDataRow[0].trim().startsWith(noQuote)){
    //  SingleNPIDataRow[0] = SingleNPIDataRow[0].substring(1);
    //}

    //REMOVE LAST Quote on the end of the CSV line
    int sizeOfStringArray = SingleNPIDataRow.length;
    //need to check to see if the size of array is > 0, then do logic
    sizeOfStringArray = sizeOfStringArray -1;
    if(SingleNPIDataRow[sizeOfStringArray].contains(noQuote)) {
      int singleStringLength = SingleNPIDataRow[sizeOfStringArray].length();
      if(singleStringLength > 0) {
        SingleNPIDataRow[sizeOfStringArray] = SingleNPIDataRow[sizeOfStringArray].substring(0, singleStringLength - 1);
      }
    }


    StringBuilder sb = new StringBuilder();
    sb.append("<EIPData><RECORDS><DATA>");

    for(int i = 0; i < beginElementNameList.size(); i++){
      //REMOVE RANDOM UNCLOSED XML CONTENT IN FILE
      if(SingleNPIDataRow[i].trim().contains(noNeed)) {
        SingleNPIDataRow[i] = "";
      }
      //remove amp, causes break
      String my_new_str = SingleNPIDataRow[i].trim().replaceAll("&", "&amp;");
      SingleNPIDataRow[i] = my_new_str;
      //append
      //added trim 3/30 remove blank white spaces
      //added remove 3/30 empty elements
      if(SingleNPIDataRow[i].length() > 0) {
        sb.append(beginElementNameList.get(i)).append(SingleNPIDataRow[i].trim()).append(endElementNameList.get(i));
      }
      //empty
      my_new_str ="";
    }
    sb.append("</DATA></RECORDS></EIPData>");

    return sb.toString();
  }

  public static ArrayList<String> constructBeginList() {
    ArrayList<String> beginElementNameList = new ArrayList<String>();
    beginElementNameList.add("<NPI>");
    beginElementNameList.add("<ETC>");
    beginElementNameList.add("<RN>");
    beginElementNameList.add("<EIN>");
    beginElementNameList.add("<PONBN>");
    beginElementNameList.add("<PLNN>");
    beginElementNameList.add("<PFN>");
    beginElementNameList.add("<PMN>");
    beginElementNameList.add("<PNPT>");
    beginElementNameList.add("<PNST>");
    beginElementNameList.add("<PCT>");
    beginElementNameList.add("<POON>");
    beginElementNameList.add("<POONTC>");
    beginElementNameList.add("<POLN>");
    beginElementNameList.add("<POFN>");
    beginElementNameList.add("<POMN>");
    beginElementNameList.add("<PONPT>");
    beginElementNameList.add("<PONST>");
    beginElementNameList.add("<POCT>");
    beginElementNameList.add("<POLNTC>");
    beginElementNameList.add("<PFLBMA>");
    beginElementNameList.add("<PSLBMA>");
    beginElementNameList.add("<PBMACN>");
    beginElementNameList.add("<PBMASN>");
    beginElementNameList.add("<PBMAPC>");
    beginElementNameList.add("<PBMACCOU>");
    beginElementNameList.add("<PBMATN>");
    beginElementNameList.add("<PBMAFN>");
    beginElementNameList.add("<PFLBPLA>");
    beginElementNameList.add("<PSLBPLA>");
    beginElementNameList.add("<PBPLACN>");
    beginElementNameList.add("<PBPLASN>");
    beginElementNameList.add("<PBPLAPC>");
    beginElementNameList.add("<PBPLACCOU>");
    beginElementNameList.add("<PBPLATN>");
    beginElementNameList.add("<PBPLAFN>");
    beginElementNameList.add("<PED>");
    beginElementNameList.add("<LUD>");
    beginElementNameList.add("<NDRC>");
    beginElementNameList.add("<NDD>");
    beginElementNameList.add("<NRD>");
    beginElementNameList.add("<PGC>");
    beginElementNameList.add("<AOLN>");
    beginElementNameList.add("<AOFN>");
    beginElementNameList.add("<AOMN>");
    beginElementNameList.add("<AOTOP>");
    beginElementNameList.add("<AOTN>");
    beginElementNameList.add("<HPTC1>");
    beginElementNameList.add("<PLN1>");
    beginElementNameList.add("<PLNSC1>");
    beginElementNameList.add("<HPPTS1>");
    beginElementNameList.add("<HPTC2>");
    beginElementNameList.add("<PLN2>");
    beginElementNameList.add("<PLNSC2>");
    beginElementNameList.add("<HPPTS2>");
    beginElementNameList.add("<HPTC3>");
    beginElementNameList.add("<PLN3>");
    beginElementNameList.add("<PLNSC3>");
    beginElementNameList.add("<HPPTS3>");
    beginElementNameList.add("<HPTC4>");
    beginElementNameList.add("<PLN4>");
    beginElementNameList.add("<PLNSC4>");
    beginElementNameList.add("<HPPTS4>");
    beginElementNameList.add("<HPTC5>");
    beginElementNameList.add("<PLN5>");
    beginElementNameList.add("<PLNSC5>");
    beginElementNameList.add("<HPPTS5>");
    beginElementNameList.add("<HPTC6>");
    beginElementNameList.add("<PLN6>");
    beginElementNameList.add("<PLNSC6>");
    beginElementNameList.add("<HPPTS6>");
    beginElementNameList.add("<HPTC7>");
    beginElementNameList.add("<PLN7>");
    beginElementNameList.add("<PLNSC7>");
    beginElementNameList.add("<HPPTS7>");
    beginElementNameList.add("<HPTC8>");
    beginElementNameList.add("<PLN8>");
    beginElementNameList.add("<PLNSC8>");
    beginElementNameList.add("<HPPTS8>");
    beginElementNameList.add("<HPTC9>");
    beginElementNameList.add("<PLN9>");
    beginElementNameList.add("<PLNSC9>");
    beginElementNameList.add("<HPPTS9>");
    beginElementNameList.add("<HPTC10>");
    beginElementNameList.add("<PLN10>");
    beginElementNameList.add("<PLNSC10>");
    beginElementNameList.add("<HPPTS10>");
    beginElementNameList.add("<HPTC11>");
    beginElementNameList.add("<PLN11>");
    beginElementNameList.add("<PLNSC11>");
    beginElementNameList.add("<HPPTS11>");
    beginElementNameList.add("<HPTC12>");
    beginElementNameList.add("<PLN12>");
    beginElementNameList.add("<PLNSC12>");
    beginElementNameList.add("<HPPTS12>");
    beginElementNameList.add("<HPTC13>");
    beginElementNameList.add("<PLN13>");
    beginElementNameList.add("<PLNSC13>");
    beginElementNameList.add("<HPPTS13>");
    beginElementNameList.add("<HPTC14>");
    beginElementNameList.add("<PLN14>");
    beginElementNameList.add("<PLNSC14>");
    beginElementNameList.add("<HPPTS14>");
    beginElementNameList.add("<HPTC15>");
    beginElementNameList.add("<PLN15>");
    beginElementNameList.add("<PLNSC15>");
    beginElementNameList.add("<HPPTS15>");
    beginElementNameList.add("<OPI1>");
    beginElementNameList.add("<OPITC1>");
    beginElementNameList.add("<OPIS1>");
    beginElementNameList.add("<OPII1>");
    beginElementNameList.add("<OPI2>");
    beginElementNameList.add("<OPITC2>");
    beginElementNameList.add("<OPIS2>");
    beginElementNameList.add("<OPII2>");
    beginElementNameList.add("<OPI3>");
    beginElementNameList.add("<OPITC3>");
    beginElementNameList.add("<OPIS3>");
    beginElementNameList.add("<OPII3>");
    beginElementNameList.add("<OPI4>");
    beginElementNameList.add("<OPITC4>");
    beginElementNameList.add("<OPIS4>");
    beginElementNameList.add("<OPII4>");
    beginElementNameList.add("<OPI5>");
    beginElementNameList.add("<OPITC5>");
    beginElementNameList.add("<OPIS5>");
    beginElementNameList.add("<OPII5>");
    beginElementNameList.add("<OPI6>");
    beginElementNameList.add("<OPITC6>");
    beginElementNameList.add("<OPIS6>");
    beginElementNameList.add("<OPII6>");
    beginElementNameList.add("<OPI7>");
    beginElementNameList.add("<OPITC7>");
    beginElementNameList.add("<OPIS7>");
    beginElementNameList.add("<OPII7>");
    beginElementNameList.add("<OPI8>");
    beginElementNameList.add("<OPITC8>");
    beginElementNameList.add("<OPIS8>");
    beginElementNameList.add("<OPII8>");
    beginElementNameList.add("<OPI9>");
    beginElementNameList.add("<OPITC9>");
    beginElementNameList.add("<OPIS9>");
    beginElementNameList.add("<OPII9>");
    beginElementNameList.add("<OPI10>");
    beginElementNameList.add("<OPITC10>");
    beginElementNameList.add("<OPIS10>");
    beginElementNameList.add("<OPII10>");
    beginElementNameList.add("<OPI11>");
    beginElementNameList.add("<OPITC11>");
    beginElementNameList.add("<OPIS11>");
    beginElementNameList.add("<OPII11>");
    beginElementNameList.add("<OPI12>");
    beginElementNameList.add("<OPITC12>");
    beginElementNameList.add("<OPIS12>");
    beginElementNameList.add("<OPII12>");
    beginElementNameList.add("<OPI13>");
    beginElementNameList.add("<OPITC13>");
    beginElementNameList.add("<OPIS13>");
    beginElementNameList.add("<OPII13>");
    beginElementNameList.add("<OPI14>");
    beginElementNameList.add("<OPITC14>");
    beginElementNameList.add("<OPIS14>");
    beginElementNameList.add("<OPII14>");
    beginElementNameList.add("<OPI15>");
    beginElementNameList.add("<OPITC15>");
    beginElementNameList.add("<OPIS15>");
    beginElementNameList.add("<OPII15>");
    beginElementNameList.add("<OPI16>");
    beginElementNameList.add("<OPITC16>");
    beginElementNameList.add("<OPIS16>");
    beginElementNameList.add("<OPII16>");
    beginElementNameList.add("<OPI17>");
    beginElementNameList.add("<OPITC17>");
    beginElementNameList.add("<OPIS17>");
    beginElementNameList.add("<OPII17>");
    beginElementNameList.add("<OPI18>");
    beginElementNameList.add("<OPITC18>");
    beginElementNameList.add("<OPIS18>");
    beginElementNameList.add("<OPII18>");
    beginElementNameList.add("<OPI19>");
    beginElementNameList.add("<OPITC19>");
    beginElementNameList.add("<OPIS19>");
    beginElementNameList.add("<OPII19>");
    beginElementNameList.add("<OPI20>");
    beginElementNameList.add("<OPITC20>");
    beginElementNameList.add("<OPIS20>");
    beginElementNameList.add("<OPII20>");
    beginElementNameList.add("<OPI21>");
    beginElementNameList.add("<OPITC21>");
    beginElementNameList.add("<OPIS21>");
    beginElementNameList.add("<OPII21>");
    beginElementNameList.add("<OPI22>");
    beginElementNameList.add("<OPITC22>");
    beginElementNameList.add("<OPIS22>");
    beginElementNameList.add("<OPII22>");
    beginElementNameList.add("<OPI23>");
    beginElementNameList.add("<OPITC23>");
    beginElementNameList.add("<OPIS23>");
    beginElementNameList.add("<OPII23>");
    beginElementNameList.add("<OPI24>");
    beginElementNameList.add("<OPITC24>");
    beginElementNameList.add("<OPIS24>");
    beginElementNameList.add("<OPII24>");
    beginElementNameList.add("<OPI25>");
    beginElementNameList.add("<OPITC25>");
    beginElementNameList.add("<OPIS25>");
    beginElementNameList.add("<OPII25>");
    beginElementNameList.add("<OPI26>");
    beginElementNameList.add("<OPITC26>");
    beginElementNameList.add("<OPIS26>");
    beginElementNameList.add("<OPII26>");
    beginElementNameList.add("<OPI27>");
    beginElementNameList.add("<OPITC27>");
    beginElementNameList.add("<OPIS27>");
    beginElementNameList.add("<OPII27>");
    beginElementNameList.add("<OPI28>");
    beginElementNameList.add("<OPITC28>");
    beginElementNameList.add("<OPIS28>");
    beginElementNameList.add("<OPII28>");
    beginElementNameList.add("<OPI29>");
    beginElementNameList.add("<OPITC29>");
    beginElementNameList.add("<OPIS29>");
    beginElementNameList.add("<OPII29>");
    beginElementNameList.add("<OPI30>");
    beginElementNameList.add("<OPITC30>");
    beginElementNameList.add("<OPIS30>");
    beginElementNameList.add("<OPII30>");
    beginElementNameList.add("<OPI31>");
    beginElementNameList.add("<OPITC31>");
    beginElementNameList.add("<OPIS31>");
    beginElementNameList.add("<OPII31>");
    beginElementNameList.add("<OPI32>");
    beginElementNameList.add("<OPITC32>");
    beginElementNameList.add("<OPIS32>");
    beginElementNameList.add("<OPII32>");
    beginElementNameList.add("<OPI33>");
    beginElementNameList.add("<OPITC33>");
    beginElementNameList.add("<OPIS33>");
    beginElementNameList.add("<OPII33>");
    beginElementNameList.add("<OPI34>");
    beginElementNameList.add("<OPITC34>");
    beginElementNameList.add("<OPIS34>");
    beginElementNameList.add("<OPII34>");
    beginElementNameList.add("<OPI35>");
    beginElementNameList.add("<OPITC35>");
    beginElementNameList.add("<OPIS35>");
    beginElementNameList.add("<OPII35>");
    beginElementNameList.add("<OPI36>");
    beginElementNameList.add("<OPITC36>");
    beginElementNameList.add("<OPIS36>");
    beginElementNameList.add("<OPII36>");
    beginElementNameList.add("<OPI37>");
    beginElementNameList.add("<OPITC37>");
    beginElementNameList.add("<OPIS37>");
    beginElementNameList.add("<OPII37>");
    beginElementNameList.add("<OPI38>");
    beginElementNameList.add("<OPITC38>");
    beginElementNameList.add("<OPIS38>");
    beginElementNameList.add("<OPII38>");
    beginElementNameList.add("<OPI39>");
    beginElementNameList.add("<OPITC39>");
    beginElementNameList.add("<OPIS39>");
    beginElementNameList.add("<OPII39>");
    beginElementNameList.add("<OPI40>");
    beginElementNameList.add("<OPITC40>");
    beginElementNameList.add("<OPIS40>");
    beginElementNameList.add("<OPII40>");
    beginElementNameList.add("<OPI41>");
    beginElementNameList.add("<OPITC41>");
    beginElementNameList.add("<OPIS41>");
    beginElementNameList.add("<OPII41>");
    beginElementNameList.add("<OPI42>");
    beginElementNameList.add("<OPITC42>");
    beginElementNameList.add("<OPIS42>");
    beginElementNameList.add("<OPII42>");
    beginElementNameList.add("<OPI43>");
    beginElementNameList.add("<OPITC43>");
    beginElementNameList.add("<OPIS43>");
    beginElementNameList.add("<OPII43>");
    beginElementNameList.add("<OPI44>");
    beginElementNameList.add("<OPITC44>");
    beginElementNameList.add("<OPIS44>");
    beginElementNameList.add("<OPII44>");
    beginElementNameList.add("<OPI45>");
    beginElementNameList.add("<OPITC45>");
    beginElementNameList.add("<OPIS45>");
    beginElementNameList.add("<OPII45>");
    beginElementNameList.add("<OPI46>");
    beginElementNameList.add("<OPITC46>");
    beginElementNameList.add("<OPIS46>");
    beginElementNameList.add("<OPII46>");
    beginElementNameList.add("<OPI47>");
    beginElementNameList.add("<OPITC47>");
    beginElementNameList.add("<OPIS47>");
    beginElementNameList.add("<OPII47>");
    beginElementNameList.add("<OPI48>");
    beginElementNameList.add("<OPITC48>");
    beginElementNameList.add("<OPIS48>");
    beginElementNameList.add("<OPII48>");
    beginElementNameList.add("<OPI49>");
    beginElementNameList.add("<OPITC49>");
    beginElementNameList.add("<OPIS49>");
    beginElementNameList.add("<OPII49>");
    beginElementNameList.add("<OPI50>");
    beginElementNameList.add("<OPITC50>");
    beginElementNameList.add("<OPIS50>");
    beginElementNameList.add("<OPII50>");
    beginElementNameList.add("<ISP>");
    beginElementNameList.add("<IOS>");
    beginElementNameList.add("<POL>");
    beginElementNameList.add("<POT>");
    beginElementNameList.add("<AONPT>");
    beginElementNameList.add("<AONST>");
    beginElementNameList.add("<AOCT>");
    beginElementNameList.add("<HPTG1>");
    beginElementNameList.add("<HPTG2>");
    beginElementNameList.add("<HPTG3>");
    beginElementNameList.add("<HPTG4>");
    beginElementNameList.add("<HPTG5>");
    beginElementNameList.add("<HPTG6>");
    beginElementNameList.add("<HPTG7>");
    beginElementNameList.add("<HPTG8>");
    beginElementNameList.add("<HPTG9>");
    beginElementNameList.add("<HPTG10>");
    beginElementNameList.add("<HPTG11>");
    beginElementNameList.add("<HPTG12>");
    beginElementNameList.add("<HPTG13>");
    beginElementNameList.add("<HPTG14>");
    beginElementNameList.add("<HPTG15>");

    return beginElementNameList;
  }

  public static ArrayList<String> constructEndList() {
    ArrayList<String> endElementNameList = new ArrayList<String>();
    endElementNameList.add("</NPI>");
    endElementNameList.add("</ETC>");
    endElementNameList.add("</RN>");
    endElementNameList.add("</EIN>");
    endElementNameList.add("</PONBN>");
    endElementNameList.add("</PLNN>");
    endElementNameList.add("</PFN>");
    endElementNameList.add("</PMN>");
    endElementNameList.add("</PNPT>");
    endElementNameList.add("</PNST>");
    endElementNameList.add("</PCT>");
    endElementNameList.add("</POON>");
    endElementNameList.add("</POONTC>");
    endElementNameList.add("</POLN>");
    endElementNameList.add("</POFN>");
    endElementNameList.add("</POMN>");
    endElementNameList.add("</PONPT>");
    endElementNameList.add("</PONST>");
    endElementNameList.add("</POCT>");
    endElementNameList.add("</POLNTC>");
    endElementNameList.add("</PFLBMA>");
    endElementNameList.add("</PSLBMA>");
    endElementNameList.add("</PBMACN>");
    endElementNameList.add("</PBMASN>");
    endElementNameList.add("</PBMAPC>");
    endElementNameList.add("</PBMACCOU>");
    endElementNameList.add("</PBMATN>");
    endElementNameList.add("</PBMAFN>");
    endElementNameList.add("</PFLBPLA>");
    endElementNameList.add("</PSLBPLA>");
    endElementNameList.add("</PBPLACN>");
    endElementNameList.add("</PBPLASN>");
    endElementNameList.add("</PBPLAPC>");
    endElementNameList.add("</PBPLACCOU>");
    endElementNameList.add("</PBPLATN>");
    endElementNameList.add("</PBPLAFN>");
    endElementNameList.add("</PED>");
    endElementNameList.add("</LUD>");
    endElementNameList.add("</NDRC>");
    endElementNameList.add("</NDD>");
    endElementNameList.add("</NRD>");
    endElementNameList.add("</PGC>");
    endElementNameList.add("</AOLN>");
    endElementNameList.add("</AOFN>");
    endElementNameList.add("</AOMN>");
    endElementNameList.add("</AOTOP>");
    endElementNameList.add("</AOTN>");
    endElementNameList.add("</HPTC1>");
    endElementNameList.add("</PLN1>");
    endElementNameList.add("</PLNSC1>");
    endElementNameList.add("</HPPTS1>");
    endElementNameList.add("</HPTC2>");
    endElementNameList.add("</PLN2>");
    endElementNameList.add("</PLNSC2>");
    endElementNameList.add("</HPPTS2>");
    endElementNameList.add("</HPTC3>");
    endElementNameList.add("</PLN3>");
    endElementNameList.add("</PLNSC3>");
    endElementNameList.add("</HPPTS3>");
    endElementNameList.add("</HPTC4>");
    endElementNameList.add("</PLN4>");
    endElementNameList.add("</PLNSC4>");
    endElementNameList.add("</HPPTS4>");
    endElementNameList.add("</HPTC5>");
    endElementNameList.add("</PLN5>");
    endElementNameList.add("</PLNSC5>");
    endElementNameList.add("</HPPTS5>");
    endElementNameList.add("</HPTC6>");
    endElementNameList.add("</PLN6>");
    endElementNameList.add("</PLNSC6>");
    endElementNameList.add("</HPPTS6>");
    endElementNameList.add("</HPTC7>");
    endElementNameList.add("</PLN7>");
    endElementNameList.add("</PLNSC7>");
    endElementNameList.add("</HPPTS7>");
    endElementNameList.add("</HPTC8>");
    endElementNameList.add("</PLN8>");
    endElementNameList.add("</PLNSC8>");
    endElementNameList.add("</HPPTS8>");
    endElementNameList.add("</HPTC9>");
    endElementNameList.add("</PLN9>");
    endElementNameList.add("</PLNSC9>");
    endElementNameList.add("</HPPTS9>");
    endElementNameList.add("</HPTC10>");
    endElementNameList.add("</PLN10>");
    endElementNameList.add("</PLNSC10>");
    endElementNameList.add("</HPPTS10>");
    endElementNameList.add("</HPTC11>");
    endElementNameList.add("</PLN11>");
    endElementNameList.add("</PLNSC11>");
    endElementNameList.add("</HPPTS11>");
    endElementNameList.add("</HPTC12>");
    endElementNameList.add("</PLN12>");
    endElementNameList.add("</PLNSC12>");
    endElementNameList.add("</HPPTS12>");
    endElementNameList.add("</HPTC13>");
    endElementNameList.add("</PLN13>");
    endElementNameList.add("</PLNSC13>");
    endElementNameList.add("</HPPTS13>");
    endElementNameList.add("</HPTC14>");
    endElementNameList.add("</PLN14>");
    endElementNameList.add("</PLNSC14>");
    endElementNameList.add("</HPPTS14>");
    endElementNameList.add("</HPTC15>");
    endElementNameList.add("</PLN15>");
    endElementNameList.add("</PLNSC15>");
    endElementNameList.add("</HPPTS15>");
    endElementNameList.add("</OPI1>");
    endElementNameList.add("</OPITC1>");
    endElementNameList.add("</OPIS1>");
    endElementNameList.add("</OPII1>");
    endElementNameList.add("</OPI2>");
    endElementNameList.add("</OPITC2>");
    endElementNameList.add("</OPIS2>");
    endElementNameList.add("</OPII2>");
    endElementNameList.add("</OPI3>");
    endElementNameList.add("</OPITC3>");
    endElementNameList.add("</OPIS3>");
    endElementNameList.add("</OPII3>");
    endElementNameList.add("</OPI4>");
    endElementNameList.add("</OPITC4>");
    endElementNameList.add("</OPIS4>");
    endElementNameList.add("</OPII4>");
    endElementNameList.add("</OPI5>");
    endElementNameList.add("</OPITC5>");
    endElementNameList.add("</OPIS5>");
    endElementNameList.add("</OPII5>");
    endElementNameList.add("</OPI6>");
    endElementNameList.add("</OPITC6>");
    endElementNameList.add("</OPIS6>");
    endElementNameList.add("</OPII6>");
    endElementNameList.add("</OPI7>");
    endElementNameList.add("</OPITC7>");
    endElementNameList.add("</OPIS7>");
    endElementNameList.add("</OPII7>");
    endElementNameList.add("</OPI8>");
    endElementNameList.add("</OPITC8>");
    endElementNameList.add("</OPIS8>");
    endElementNameList.add("</OPII8>");
    endElementNameList.add("</OPI9>");
    endElementNameList.add("</OPITC9>");
    endElementNameList.add("</OPIS9>");
    endElementNameList.add("</OPII9>");
    endElementNameList.add("</OPI10>");
    endElementNameList.add("</OPITC10>");
    endElementNameList.add("</OPIS10>");
    endElementNameList.add("</OPII10>");
    endElementNameList.add("</OPI11>");
    endElementNameList.add("</OPITC11>");
    endElementNameList.add("</OPIS11>");
    endElementNameList.add("</OPII11>");
    endElementNameList.add("</OPI12>");
    endElementNameList.add("</OPITC12>");
    endElementNameList.add("</OPIS12>");
    endElementNameList.add("</OPII12>");
    endElementNameList.add("</OPI13>");
    endElementNameList.add("</OPITC13>");
    endElementNameList.add("</OPIS13>");
    endElementNameList.add("</OPII13>");
    endElementNameList.add("</OPI14>");
    endElementNameList.add("</OPITC14>");
    endElementNameList.add("</OPIS14>");
    endElementNameList.add("</OPII14>");
    endElementNameList.add("</OPI15>");
    endElementNameList.add("</OPITC15>");
    endElementNameList.add("</OPIS15>");
    endElementNameList.add("</OPII15>");
    endElementNameList.add("</OPI16>");
    endElementNameList.add("</OPITC16>");
    endElementNameList.add("</OPIS16>");
    endElementNameList.add("</OPII16>");
    endElementNameList.add("</OPI17>");
    endElementNameList.add("</OPITC17>");
    endElementNameList.add("</OPIS17>");
    endElementNameList.add("</OPII17>");
    endElementNameList.add("</OPI18>");
    endElementNameList.add("</OPITC18>");
    endElementNameList.add("</OPIS18>");
    endElementNameList.add("</OPII18>");
    endElementNameList.add("</OPI19>");
    endElementNameList.add("</OPITC19>");
    endElementNameList.add("</OPIS19>");
    endElementNameList.add("</OPII19>");
    endElementNameList.add("</OPI20>");
    endElementNameList.add("</OPITC20>");
    endElementNameList.add("</OPIS20>");
    endElementNameList.add("</OPII20>");
    endElementNameList.add("</OPI21>");
    endElementNameList.add("</OPITC21>");
    endElementNameList.add("</OPIS21>");
    endElementNameList.add("</OPII21>");
    endElementNameList.add("</OPI22>");
    endElementNameList.add("</OPITC22>");
    endElementNameList.add("</OPIS22>");
    endElementNameList.add("</OPII22>");
    endElementNameList.add("</OPI23>");
    endElementNameList.add("</OPITC23>");
    endElementNameList.add("</OPIS23>");
    endElementNameList.add("</OPII23>");
    endElementNameList.add("</OPI24>");
    endElementNameList.add("</OPITC24>");
    endElementNameList.add("</OPIS24>");
    endElementNameList.add("</OPII24>");
    endElementNameList.add("</OPI25>");
    endElementNameList.add("</OPITC25>");
    endElementNameList.add("</OPIS25>");
    endElementNameList.add("</OPII25>");
    endElementNameList.add("</OPI26>");
    endElementNameList.add("</OPITC26>");
    endElementNameList.add("</OPIS26>");
    endElementNameList.add("</OPII26>");
    endElementNameList.add("</OPI27>");
    endElementNameList.add("</OPITC27>");
    endElementNameList.add("</OPIS27>");
    endElementNameList.add("</OPII27>");
    endElementNameList.add("</OPI28>");
    endElementNameList.add("</OPITC28>");
    endElementNameList.add("</OPIS28>");
    endElementNameList.add("</OPII28>");
    endElementNameList.add("</OPI29>");
    endElementNameList.add("</OPITC29>");
    endElementNameList.add("</OPIS29>");
    endElementNameList.add("</OPII29>");
    endElementNameList.add("</OPI30>");
    endElementNameList.add("</OPITC30>");
    endElementNameList.add("</OPIS30>");
    endElementNameList.add("</OPII30>");
    endElementNameList.add("</OPI31>");
    endElementNameList.add("</OPITC31>");
    endElementNameList.add("</OPIS31>");
    endElementNameList.add("</OPII31>");
    endElementNameList.add("</OPI32>");
    endElementNameList.add("</OPITC32>");
    endElementNameList.add("</OPIS32>");
    endElementNameList.add("</OPII32>");
    endElementNameList.add("</OPI33>");
    endElementNameList.add("</OPITC33>");
    endElementNameList.add("</OPIS33>");
    endElementNameList.add("</OPII33>");
    endElementNameList.add("</OPI34>");
    endElementNameList.add("</OPITC34>");
    endElementNameList.add("</OPIS34>");
    endElementNameList.add("</OPII34>");
    endElementNameList.add("</OPI35>");
    endElementNameList.add("</OPITC35>");
    endElementNameList.add("</OPIS35>");
    endElementNameList.add("</OPII35>");
    endElementNameList.add("</OPI36>");
    endElementNameList.add("</OPITC36>");
    endElementNameList.add("</OPIS36>");
    endElementNameList.add("</OPII36>");
    endElementNameList.add("</OPI37>");
    endElementNameList.add("</OPITC37>");
    endElementNameList.add("</OPIS37>");
    endElementNameList.add("</OPII37>");
    endElementNameList.add("</OPI38>");
    endElementNameList.add("</OPITC38>");
    endElementNameList.add("</OPIS38>");
    endElementNameList.add("</OPII38>");
    endElementNameList.add("</OPI39>");
    endElementNameList.add("</OPITC39>");
    endElementNameList.add("</OPIS39>");
    endElementNameList.add("</OPII39>");
    endElementNameList.add("</OPI40>");
    endElementNameList.add("</OPITC40>");
    endElementNameList.add("</OPIS40>");
    endElementNameList.add("</OPII40>");
    endElementNameList.add("</OPI41>");
    endElementNameList.add("</OPITC41>");
    endElementNameList.add("</OPIS41>");
    endElementNameList.add("</OPII41>");
    endElementNameList.add("</OPI42>");
    endElementNameList.add("</OPITC42>");
    endElementNameList.add("</OPIS42>");
    endElementNameList.add("</OPII42>");
    endElementNameList.add("</OPI43>");
    endElementNameList.add("</OPITC43>");
    endElementNameList.add("</OPIS43>");
    endElementNameList.add("</OPII43>");
    endElementNameList.add("</OPI44>");
    endElementNameList.add("</OPITC44>");
    endElementNameList.add("</OPIS44>");
    endElementNameList.add("</OPII44>");
    endElementNameList.add("</OPI45>");
    endElementNameList.add("</OPITC45>");
    endElementNameList.add("</OPIS45>");
    endElementNameList.add("</OPII45>");
    endElementNameList.add("</OPI46>");
    endElementNameList.add("</OPITC46>");
    endElementNameList.add("</OPIS46>");
    endElementNameList.add("</OPII46>");
    endElementNameList.add("</OPI47>");
    endElementNameList.add("</OPITC47>");
    endElementNameList.add("</OPIS47>");
    endElementNameList.add("</OPII47>");
    endElementNameList.add("</OPI48>");
    endElementNameList.add("</OPITC48>");
    endElementNameList.add("</OPIS48>");
    endElementNameList.add("</OPII48>");
    endElementNameList.add("</OPI49>");
    endElementNameList.add("</OPITC49>");
    endElementNameList.add("</OPIS49>");
    endElementNameList.add("</OPII49>");
    endElementNameList.add("</OPI50>");
    endElementNameList.add("</OPITC50>");
    endElementNameList.add("</OPIS50>");
    endElementNameList.add("</OPII50>");
    endElementNameList.add("</ISP>");
    endElementNameList.add("</IOS>");
    endElementNameList.add("</POL>");
    endElementNameList.add("</POT>");
    endElementNameList.add("</AONPT>");
    endElementNameList.add("</AONST>");
    endElementNameList.add("</AOCT>");
    endElementNameList.add("</HPTG1>");
    endElementNameList.add("</HPTG2>");
    endElementNameList.add("</HPTG3>");
    endElementNameList.add("</HPTG4>");
    endElementNameList.add("</HPTG5>");
    endElementNameList.add("</HPTG6>");
    endElementNameList.add("</HPTG7>");
    endElementNameList.add("</HPTG8>");
    endElementNameList.add("</HPTG9>");
    endElementNameList.add("</HPTG10>");
    endElementNameList.add("</HPTG11>");
    endElementNameList.add("</HPTG12>");
    endElementNameList.add("</HPTG13>");
    endElementNameList.add("</HPTG14>");
    endElementNameList.add("</HPTG15>");

    return endElementNameList;

  }
}
