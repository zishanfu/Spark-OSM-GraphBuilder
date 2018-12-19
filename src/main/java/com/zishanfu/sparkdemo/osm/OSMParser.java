package com.zishanfu.sparkdemo.osm;


import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.pbf2.v0_6.PbfReader;

import com.zishanfu.sparkdemo.entity.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;


public class OSMParser {
	Set<String> allowableWays = new HashSet<>(Arrays.asList(  
			  "motorway",
			  "motorway_link",
			  "trunk",
			  "trunk_link",
			  "primary",
			  "primary_link",
			  "secondary",
			  "secondary_link",
			  "tertiary",
			  "tertiary_link",
			  "living_street",
			  "residential",
			  "road",
			  "construction",
			  "motorway_junction"));
	
	public OSMParser(String osm) {
		SparkSession spark = SparkSession
	    		  .builder()
	    		  .master("local")
	    		  .appName("OSMParser")
	    		  .getOrCreate();
		SparkContext sc = spark.sparkContext();
		
		File osmFile = new File(osm);
        PbfReader reader = new PbfReader(osmFile, 1);
        List<NodeEntry> nodes = new ArrayList<>();
        List<WayEntry> ways = new ArrayList<>();
        List<Relation> relations = new ArrayList<>();
 
        Sink sinkImplementation = new Sink() {
 
            public void process(EntityContainer entityContainer) {
 
                Entity entity = entityContainer.getEntity();
                List<String> tags = entity.getTags().stream().map(Tag::getValue).collect(Collectors.toList());
                
                if (entity instanceof Node) {
                	
                	nodes.add(new NodeEntry(entity.getId(), 
                			((Node) entity).getLatitude(),((Node) entity).getLongitude(), tags));
                	
                } else if (entity instanceof Way) {
                	ways.add(new WayEntry(entity.getId(), tags, ((Way) entity).getWayNodes().stream().map(WayNode::getNodeId).collect(Collectors.toList())));
                	
                } else if (entity instanceof Relation) {
                	
                	Set<String> tagSet = entity.getTags().stream().map(Tag::getValue).collect(Collectors.toSet());
            		if(tagSet.retainAll(allowableWays) && tagSet.size() != 0) {
            			relations.add((Relation) entity);
            		}
            		
                }
                
            }
 
            public void initialize(Map<String, Object> arg0) {
            }
 
            public void complete() {
            }
 
            public void release() {
            }
 
        };
 
        reader.setSink(sinkImplementation);
        reader.run();
        
        Encoder<NodeEntry> nodeEncoder = Encoders.bean(NodeEntry.class);
        Dataset<NodeEntry> nodeDS = spark.createDataset(nodes, nodeEncoder);
        
        Encoder<WayEntry> wayEncoder = Encoders.bean(WayEntry.class);
        Dataset<WayEntry> wayDS = spark.createDataset(ways, wayEncoder);
        
        Encoder<Long> longEncoder = Encoders.LONG();
        
        Dataset<Long> wayNodeIds = wayDS.flatMap(w -> {
        	return w.getNodes().iterator();
        }, longEncoder).coalesce(1);

        Dataset<Row> intersectionNodes = wayNodeIds.groupBy("value").count().filter("count >= 2").select("value");
        		
        Dataset<Row> wayNodes = nodeDS.joinWith(wayNodeIds.distinct(), wayNodeIds.distinct().col("value").equalTo(nodeDS.col("nodeId"))).select("nodeId").cache();
        wayNodes.show();
        spark.stop();
        
	}
}
