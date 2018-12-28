package com.zishanfu.sparkdemo.osm;


import java.io.File;
import java.io.Serializable;
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
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.Pregel;
import org.apache.spark.graphx.lib.ShortestPaths;
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
import com.zishanfu.sparkdemo.serializable.AbsDistFunc;
import com.zishanfu.sparkdemo.serializable.MergeMsg;
import com.zishanfu.sparkdemo.serializable.SendMsg;
import com.zishanfu.sparkdemo.serializable.SerializableFunction2;
import com.zishanfu.sparkdemo.serializable.Vprog;

import scala.Predef;
import scala.Predef.$eq$colon$eq;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction2;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class OSMParser implements Serializable{
	
	private static final long serialVersionUID = -7050802408177506358L;

	private Set<String> allowableWays = new HashSet<>(Arrays.asList(  
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
	
	private static final ClassTag<Double> tagDouble = ClassTag$.MODULE$.apply(Double.class);
	private static final ClassTag<Long> tagLong = ClassTag$.MODULE$.apply(Long.class);
	
	private static final $eq$colon$eq<Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>>, Tuple2<Double, ArrayList<Long>>> eqMap = new $eq$colon$eq<Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>>, Tuple2<Double, ArrayList<Long>>>(){
		private static final long serialVersionUID = 1L;

		public Tuple2<Double, ArrayList<Long>> apply(Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>> arg0) {
			     return new Tuple2<>(Double.POSITIVE_INFINITY, Lists.newArrayList());
		};
	};
	
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
			public void complete() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void release() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void initialize(java.util.Map<String, Object> metaData) {
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
//        JavaPairRDD<Object, Map<Long, Tuple2<List<Long>, List<Long>>>> intersectVertices = segmentWaysDS.toJavaRDD().mapToPair(sw -> {
//        	List<Tuple2<Long, Tuple2<List<Long>, List<Long>>>> tmp = new ArrayList<>();
//        	tmp.add(new Tuple2<>(sw._1, new Tuple2<>(sw._2.getInBuf(), sw._2.getOutBuf())));
//        	Seq<Tuple2<Long, Tuple2<List<Long>, List<Long>>>> tmpSeq = JavaConverters.asScalaIteratorConverter(tmp.iterator()).asScala().toSeq();
//        	return new Tuple2<>((Object)sw._2.getOSMId(), (Map<Long, Tuple2<List<Long>, List<Long>>>) scala.collection.mutable.Map$.MODULE$.apply(tmpSeq));
//        }).reduceByKey((a, b) -> {
//        	a.$plus$plus(b);
//        	//a.putAll(b);
//        	return a;
//        });
        JavaPairRDD<Object, Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>>> intersectVertices = segmentWaysDS.toJavaRDD().mapToPair(sw -> {
    			Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>> map = new HashMap<>();
    			map.put(sw._1, new Tuple2<>(Lists.newArrayList(sw._2.getInBuf()), Lists.newArrayList(sw._2.getOutBuf())));
    			return new Tuple2<>((Object)sw._2.getOSMId(), map);
        }).reduceByKey((a, b) -> {
        		a.putAll(b);
        		return a;
        });
        
//        intersectVertices.take(10).forEach(iv ->{
//        		System.out.println(iv._1 + "," + iv._2);
//        });
        
        JavaRDD<Edge<Long>> edges = segmentedWays.filter(way -> way._2.size() > 1).flatMap(way -> {
        	return new SlidingList<Tuple3<Long, List<Long>, List<Long>>>(way._2).windows(2).stream().flatMap(segment ->{
        		return Lists.newArrayList(Arrays.asList(
        				new Edge<>(segment._1._1(), segment._2._1(), way._1),
        				new Edge<>(segment._2._1(), segment._1._1(), way._1)
        				)).stream();
        		}).collect(Collectors.toList()).iterator();
        });
        
//        edges.take(10).forEach(e -> {
//        	System.out.println(e.toString());
//        });
        Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>> initMap = ImmutableMap.<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>>builder()
        		.put(1L, new Tuple2<ArrayList<Long>, ArrayList<Long>>(new ArrayList<>(), new ArrayList<>())).build();
        Graph<Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>>, Long> roadGraph = Graph.apply(
        			intersectVertices.rdd(), 
        			edges.rdd(), 
        			initMap,
        			StorageLevel.MEMORY_AND_DISK(), 
        			StorageLevel.MEMORY_AND_DISK(),
        			ClassTag$.MODULE$.apply(Map.class),
        			tagLong);
        
//        roadGraph.edges().toJavaRDD().foreach(r -> {
//        		System.out.println(r);
//        });
//        roadGraph.vertices().toJavaRDD().foreach(r -> {
//    			System.out.println(r);
//        });

        java.util.Map<Long, Tuple2<Double, Double>> OSMNodes = wayNodes.javaRDD().mapToPair(node ->{
        		return new Tuple2<>(node.getNodeId(), new Tuple2<>(node.getLat(), node.getLon()));
        }).collectAsMap();
        

        Graph<Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>>, Tuple2<Long, Double>> weightedRoadGraph = roadGraph.mapTriplets(
        		new AbsDistFunc(OSMNodes), scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class));
        
        //32884939,32884943
        Seq<Long> request = convertListToSeq(Arrays.asList(32884939L,32884943L));
        Graph<Tuple2<Double, ArrayList<Long>>, Tuple2<Long, Double>> initalGraph = weightedRoadGraph.mapVertices(
        		new SerializableFunction2(request), ClassTag$.MODULE$.apply(Tuple2.class), eqMap);
//
//		Pregel.apply(initalGraph, 
//				new Tuple2<Double, ArrayList<Long>>(Double.POSITIVE_INFINITY, Lists.newArrayList()), 
//				Integer.MAX_VALUE, 
//				EdgeDirection.Out(), 
//				new Vprog(), 
//				new SendMsg(), 
//				new MergeMsg(), 
//				scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class), 
//				scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class), 
//				scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class));
        spark.stop();
        
	}
	
	public static Seq<Long> convertListToSeq(List<Long> inputList) {
	    return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}
	

	
	
}
