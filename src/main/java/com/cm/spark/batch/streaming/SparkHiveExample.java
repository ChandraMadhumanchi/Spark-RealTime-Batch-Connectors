package com.cm.spark.batch.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.hive.HiveContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

@SuppressWarnings("deprecation")
public class SparkHiveExample {
	  public static void main(String[] args) {
	    //SparkConf conf = new SparkConf().setAppName("SparkHive Example").setMaster("local[2]");
	    //SparkContext sc = new SparkContext(conf);
	    //HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc);
	    //Dataset df = hiveContext.sql("show tables");
	    //df.show();
	    /*
	     * JSONParser parser = new JSONParser();
					JSONObject json = (JSONObject) parser.parse(cr.value().toString());
					JSONObject innerObject = (JSONObject) json.get("IXRetail");
					JSONObject MsgDataObject = (JSONObject) innerObject.get("MsgData");
					// loop array
					JSONArray jsonArray = (JSONArray) MsgDataObject.get("Transaction");

					for (int i = 0; i < jsonArray.size(); i++) {
						// System.out.println(jsonArray.get(i));

						String jsonData = "{\"IXRetail\":" + "{\"MsgData\":" + "{\"Transaction\":"
								+ jsonArray.get(i).toString() + "}" + "}" + "}";
						System.out.println(jsonData);

					}
					
					
					/*
		 * // Read value of each message from Kafka and return it
		 * JavaDStream<String> lines = stream.map(new
		 * Function<ConsumerRecord<String,String>, String>() {
		 * 
		 * @Override public String call(ConsumerRecord<String, String>
		 * kafkaRecord) throws Exception { return kafkaRecord.value(); } });
		 
	     */
		  
		  
	
	  }
	}
