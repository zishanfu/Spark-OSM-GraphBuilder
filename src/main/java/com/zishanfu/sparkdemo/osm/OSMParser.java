package com.zishanfu.sparkdemo.osm;


import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.pbf2.v0_6.PbfReader;

import com.zishanfu.sparkdemo.entity.Intersection;
import com.zishanfu.sparkdemo.entity.IntersectionNode;
import com.zishanfu.sparkdemo.entity.LabeledWay;
import com.zishanfu.sparkdemo.entity.NodeEntry;
import com.zishanfu.sparkdemo.entity.WayEntry;

import scala.Tuple2;
import scala.Tuple3;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag;


public class OSMParser{
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
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		
		//Digest OSM data
		File osmFile = new File(osm);
        PbfReader reader = new PbfReader(osmFile, 1);
        ArrayBuffer<NodeEntry> nodes = new ArrayBuffer<>();
        ArrayBuffer<WayEntry> ways = new ArrayBuffer<>();
        ArrayBuffer<Relation> relations = new ArrayBuffer<>();
 
        Sink sinkImplementation = new Sink() {
 
            public void process(EntityContainer entityContainer) {
 
                Entity entity = entityContainer.getEntity();
                List<String> tags = entity.getTags().stream().map(Tag::getValue).collect(Collectors.toList());
                
                if (entity instanceof Node) {
                	nodes.$plus$eq(new NodeEntry(entity.getId(), 
                			((Node) entity).getLatitude(),((Node) entity).getLongitude(), tags));
                	
                } else if (entity instanceof Way) {
                	ways.$plus$eq(new WayEntry(entity.getId(), tags, ((Way) entity).getWayNodes().stream().map(WayNode::getNodeId).collect(Collectors.toList())));
                	
                } else if (entity instanceof Relation) {
                	
                	Set<String> tagSet = entity.getTags().stream().map(Tag::getValue).collect(Collectors.toSet());
            		if(tagSet.retainAll(allowableWays) && tagSet.size() != 0) {
            			relations.$plus$eq((Relation) entity);
            		}
            		
                }
                
            }

			@Override
			public void initialize(Map<String, Object> metaData) {
				// TODO Auto-generated method stub
			}

			@Override
			public void complete() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void release() {
				// TODO Auto-generated method stub
				
			}
        };
 
        reader.setSink(sinkImplementation);
        reader.run();
        
        Encoder<NodeEntry> nodeEncoder = Encoders.bean(NodeEntry.class);
        Dataset<NodeEntry> nodeDS = spark.createDataset(nodes, nodeEncoder);
        
        Encoder<WayEntry> wayEncoder = Encoders.bean(WayEntry.class);
        Dataset<WayEntry> wayDS = spark.createDataset(ways, wayEncoder);
        
        //Find intersections
        Encoder<Long> longEncoder = Encoders.LONG();
        
        Dataset<Long> wayNodeIds = wayDS.flatMap(w -> {
        		return w.getNodes().iterator();
        }, longEncoder).coalesce(1);


        Dataset<Long> intersectionNodes = wayNodeIds.groupBy("value").count()
        		.filter("count >= 2").select("value").as(longEncoder);

        Broadcast<List<Long>> broadcastInters = sc.broadcast(intersectionNodes.toJavaRDD().collect());
        
        Dataset<Row> wayNodes = nodeDS.joinWith(wayNodeIds.distinct(), 
        		wayNodeIds.distinct().col("value").equalTo(nodeDS.col("nodeId"))).select("_1").cache();
        
        
        Encoder<LabeledWay> lwEncoder = Encoders.bean(LabeledWay.class);
        Dataset<LabeledWay> labeledWays = wayDS.map(w ->{
        		List<Tuple2<Long, Boolean>> nodesWithLabels = w.getNodes().stream().map(
        				id -> new Tuple2<Long, Boolean>(id, broadcastInters.getValue().contains(id))
        				).collect(Collectors.toList());
        		nodesWithLabels.set(nodesWithLabels.size()-1, 
        				new Tuple2<Long, Boolean>(nodesWithLabels.get(nodesWithLabels.size()-1)._1, true));
        		nodesWithLabels.set(0, new Tuple2<Long, Boolean>(nodesWithLabels.get(0)._1, true));
        		return new LabeledWay(w.getWayId(), nodesWithLabels);
        }, lwEncoder);
        
        
        //Convert ways into edges
        JavaPairRDD<Long, List<Tuple3<Long, List<Long>, List<Long>>>> segmentedWays = labeledWays.javaRDD().mapToPair(lw ->{
        	return new Tuple2<Long, List<Tuple3<Long, List<Long>, List<Long>>>>(
        			lw.getWayId(), 
        			segmentWay(lw.getLabeledNodes()));
        });
        
        Encoder<Tuple2<Long, IntersectionNode>> tupleEncoder = Encoders.tuple(Encoders.LONG(), Encoders.bean(IntersectionNode.class));
        
        // for each (wayId, (inBuf, outBuf)) => (wayId, IntersectionNode(nodeId, inBuf, outBuf))


        
        spark.stop();
        
	}
	
	
	private static List<Tuple3<Long, List<Long>, List<Long>>> segmentWay(List<Tuple2<Long, Boolean>> way){
		List<Intersection> intersections = new ArrayList<Intersection>();
		List<Long> currentBuffer = new ArrayList<Long>();
		
		if(way.size() == 1) {
			Intersection intersect = new Intersection(way.get(0)._1, new ArrayList<Long>(Arrays.asList(-1L)), new ArrayList<Long>(Arrays.asList(-1L)));
			return new ArrayList<>( Arrays.asList(
					new Tuple3<>(
							intersect.getOSMId(), 
							intersect.getInBuf(), 
							intersect.getOutBuf())));
		}
		
		//Tuple2<Long, Boolean>
		//(id, isIntersection)
		for(int i = 0; i<way.size(); i++) {
			Tuple2<Long, Boolean> node = way.get(i);
			if(node._2) {
				Intersection newEntry = new Intersection(node._1, new ArrayList<>(currentBuffer), new ArrayList<>());
				intersections.add(newEntry);
				currentBuffer.clear();
			}else {
				currentBuffer.add(node._1);
			}
			
			if(i == way.size() - 1 && !currentBuffer.isEmpty()) {
				if(intersections.isEmpty()) {
					intersections.add(new Intersection(-1L, new ArrayList<Long>(), currentBuffer));
				}else {
					int last = intersections.size() - 1;
					List<Long> of = intersections.get(last).getOutBuf();
					of.addAll(currentBuffer);
					intersections.set(last, new Intersection(
							intersections.get(last).getOSMId(),
							intersections.get(last).getInBuf(),
							of));
				}
				currentBuffer.clear();
			}
		}

		
		return intersections.stream().map(i -> new Tuple3<Long, List<Long>, List<Long>>(i.getOSMId(), i.getInBuf(), i.getOutBuf())).collect(Collectors.toList());
		
	}
	
	
	
	
	
	
	
}
