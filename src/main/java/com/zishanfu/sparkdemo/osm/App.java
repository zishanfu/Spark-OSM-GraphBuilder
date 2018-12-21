package com.zishanfu.sparkdemo.osm;

public class App 
{
	static String base = System.getProperty("user.home");
	
    public static void main( String[] args )
    {
//    	SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-map-test");
//    	SparkSession ss = SparkSession.builder().config(sparkConf)
//    			.config("spark.executor.memory", "30g")
//    		     .config("spark.driver.memory", "25g")
//    		     .config("spark.memory.offHeap.enabled",true)
//    		     .config("spark.memory.offHeap.size","16g")   
//    		     .getOrCreate();
        OSMParser osmparser = new OSMParser(base + "/Downloads/datasets/maldives-latest.osm.pbf");
    }
}
