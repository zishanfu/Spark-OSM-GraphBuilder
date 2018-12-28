package com.zishanfu.sparkdemo.osm;

public class App 
{
	static String base = System.getProperty("user.home");
	
    public static void main( String[] args )
    {
    	//maldives-latest.osm.pbf
        OSMParser osmparser = new OSMParser(base + "/Downloads/datasets/faroe-islands-latest.osm.pbf");
    }
}
