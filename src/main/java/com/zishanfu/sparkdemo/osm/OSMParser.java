package com.zishanfu.sparkdemo.osm;


import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeRDD;
import org.apache.spark.graphx.Graph;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.pbf2.v0_6.PbfReader;


import com.zishanfu.sparkdemo.entity.Intersection;
import com.zishanfu.sparkdemo.entity.LabeledWay;
import com.zishanfu.sparkdemo.entity.NodeEntry;
import com.zishanfu.sparkdemo.entity.WayEntry;

import scala.Tuple2;
import scala.Tuple3;
import scala.collection.mutable.ArrayBuffer;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.GeometryEngine;


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
 
        Sink sinkImplementation = new Sink() {
 
            public void process(EntityContainer entityContainer) {
 
                Entity entity = entityContainer.getEntity();
                List<String> tags = entity.getTags().stream().map(Tag::getValue).collect(Collectors.toList());
                
                if (entity instanceof Node) {
                		nodes.$plus$eq(new NodeEntry(entity.getId(), 
                			((Node) entity).getLatitude(),((Node) entity).getLongitude(), tags));
                	
                } else if (entity instanceof Way) {
                		Set<String> tagSet = entity.getTags().stream().map(Tag::getValue).collect(Collectors.toSet());
                		if(tagSet.retainAll(allowableWays) && tagSet.size() != 0) {
                			ways.$plus$eq(new WayEntry(
                					entity.getId(), 
                					tags, 
                					((Way) entity).getWayNodes().stream().map(WayNode::getNodeId).collect(Collectors.toList())));
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
        
        Dataset<NodeEntry> wayNodes = nodeDS.joinWith(wayNodeIds.distinct(), 
        		wayNodeIds.distinct().col("value").equalTo(nodeDS.col("nodeId")))
        		.map(wn -> wn._1, nodeEncoder).cache();
        
        
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
        
        JavaPairRDD<Long, List<Tuple3<Long, List<Long>, List<Long>>>> segmentedWays = labeledWays.javaRDD().mapToPair(lw ->{
        	return new Tuple2<Long, List<Tuple3<Long, List<Long>, List<Long>>>>(
        			lw.getWayId(), 
        			new MakeSegments(lw.getLabeledNodes()).getTupleList());
        });
        
        Encoder<Tuple2<Long, Intersection>> tupleEncoder = Encoders.tuple(Encoders.LONG(), Encoders.bean(Intersection.class));
        
        
        Dataset<Tuple2<Long, Intersection>> segmentWaysDS = labeledWays.flatMap(lw ->{
        		List<Intersection> sws = new MakeSegments(lw.getLabeledNodes()).getIntersectList();
        		List<Tuple2<Long, Intersection>> res = sws.stream().map(intersect -> 
        			new Tuple2<Long, Intersection>(lw.getWayId(),intersect)).collect(Collectors.toList());
        		return res.iterator();
        }, tupleEncoder);
        
        //wayid, intersection
        //wayid, (OSMId, in, out)
        //OSMId, wayid -> (in, out), ...
        //Map((wayid -> (inArray, outArray)), (wayid -> (inArray, outArray)), ...)
        JavaPairRDD<Object, Map<Long, Tuple2<List<Long>, List<Long>>>> intersectVertices = segmentWaysDS.toJavaRDD().mapToPair(sw -> {
        		Map<Long, Tuple2<List<Long>, List<Long>>> map = new HashMap<>();
        		map.put(sw._1, new Tuple2<>(sw._2.getInBuf(), sw._2.getOutBuf()));
        		return new Tuple2<>((Object)sw._2.getOSMId(), map);
        }).reduceByKey((a, b) -> {
        		a.putAll(b);
        		return a;
        });
        
//        intersectVertices.take(10).forEach(iv ->{
//        		System.out.println(iv._1);
//        });
        
        //process the data for the edges of the graph
        
        //JavaPairRDD<Long, List<Tuple3<Long, List<Long>, List<Long>>>>
        JavaRDD<Edge<Long>> edges = segmentedWays.filter(way -> way._2.size() > 1).flatMap(way -> {
        	return sliding(way._2).stream().flatMap(segment ->{
        		return new ArrayList<>(Arrays.asList(
        				new Edge<>(segment._1._1(), segment._2._1(), way._1),
        				new Edge<>(segment._2._1(), segment._1._1(), way._1)
        				)).stream();
        		}).collect(Collectors.toList()).iterator();
        });
        
//        edges.take(10).forEach(e -> {
//        	System.out.println(e.toString());
//        });
        
        Graph<Map<Long, Tuple2<List<Long>, List<Long>>>, Long> roadGraph = Graph.apply(
        			intersectVertices.rdd(), 
        			edges.rdd(), 
        			new HashMap<>(), 
        			StorageLevel.MEMORY_AND_DISK(), 
        			StorageLevel.MEMORY_AND_DISK(),
        			scala.reflect.ClassTag$.MODULE$.apply(HashMap.class),
        			scala.reflect.ClassTag$.MODULE$.apply(Long.class));
        
//        roadGraph.edges().toJavaRDD().foreach(r -> {
//        		System.out.println(r);
//        });
        roadGraph.vertices().toJavaRDD().foreach(r -> {
    			System.out.println(r);
        });

        Map<Long, Tuple2<Double, Double>> OSMNodes = wayNodes.javaRDD().mapToPair(node ->{
        		return new Tuple2<>(node.getNodeId(), new Tuple2<>(node.getLat(), node.getLon()));
        }).collectAsMap();
        		
        
        spark.stop();
        
	}
	
	private static double dist(long n1, long n2, Map<Long, Tuple2<Double, Double>> OSMNodes) {
		Tuple2<Double, Double> n1Coord = OSMNodes.get(n1);
		Tuple2<Double, Double> n2Coord = OSMNodes.get(n2);
		Point p1 = new Point(n1Coord._1, n1Coord._2);
		Point p2 = new Point(n2Coord._1, n2Coord._2);
		return GeometryEngine.geodesicDistanceOnWGS84(p1, p2);
	}

	
	private static List<Tuple2<Tuple3<Long, List<Long>, List<Long>>, Tuple3<Long, List<Long>, List<Long>>>> sliding(List<Tuple3<Long, List<Long>, List<Long>>> list){
		List<Tuple2<Tuple3<Long, List<Long>, List<Long>>, Tuple3<Long, List<Long>, List<Long>>>> res = new ArrayList<>();
		for(int i = 1; i<list.size(); i++) {
			Tuple3<Long, List<Long>, List<Long>> t1 = list.get(i - 1);
			Tuple3<Long, List<Long>, List<Long>> t2 = list.get(i);
			res.add(new Tuple2<>(t1, t2));
		}
		return res;
	}
	
	
	
	
	
	
	
}
