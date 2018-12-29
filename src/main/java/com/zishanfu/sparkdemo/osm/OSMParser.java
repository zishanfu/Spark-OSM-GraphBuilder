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
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.graphx.Pregel;
import org.apache.spark.graphx.lib.ShortestPaths;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;


import com.zishanfu.sparkdemo.entity.Intersection;
import com.zishanfu.sparkdemo.entity.LabeledWay;
import com.zishanfu.sparkdemo.entity.NodeEntry;
import com.zishanfu.sparkdemo.entity.WayEntry;
import com.zishanfu.sparkdemo.network.OSMSink;
import com.zishanfu.sparkdemo.serializable.AbsDistFunc;
import com.zishanfu.sparkdemo.serializable.MergeMsg;
import com.zishanfu.sparkdemo.serializable.SendMsg;
import com.zishanfu.sparkdemo.serializable.SerializableFunction2;
import com.zishanfu.sparkdemo.serializable.Vprog;

import scala.Predef.$eq$colon$eq;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class OSMParser{
	
	private static final ClassTag<Long> tagLong = ClassTag$.MODULE$.apply(Long.class);
	
	private static final $eq$colon$eq<Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>>, Tuple2<Double, ArrayList<Long>>> eqMap = new $eq$colon$eq<Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>>, Tuple2<Double, ArrayList<Long>>>(){

		public Tuple2<Double, ArrayList<Long>> apply(Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>> arg0) {
			     return new Tuple2<>(Double.POSITIVE_INFINITY, Lists.newArrayList());
		};
	};
	
	public OSMParser(String osmpath) {
		SparkSession spark = SparkSession
	    		  .builder()
	    		  .master("local")
	    		  .appName("OSMParser")
	    		  .getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		Encoder<NodeEntry> nodeEncoder = Encoders.bean(NodeEntry.class);
		Encoder<WayEntry> wayEncoder = Encoders.bean(WayEntry.class);
		Encoder<LabeledWay> lwEncoder = Encoders.bean(LabeledWay.class);
		Encoder<Tuple2<Long, Intersection>> tupleEncoder = Encoders.tuple(Encoders.LONG(), Encoders.bean(Intersection.class));
		Encoder<Long> longEncoder = Encoders.LONG();
		
		
		OSMSink processOSM = new OSMSink();
		processOSM.run(osmpath);
        
        Dataset<NodeEntry> nodeDS = spark.createDataset(processOSM.getNodes(), nodeEncoder);
        Dataset<WayEntry> wayDS = spark.createDataset(processOSM.getWays(), wayEncoder);
        
        //Map all the node in way to a set
        Dataset<Long> wayNodeIds = wayDS.flatMap(w -> {
        		return w.getNodes().iterator();
        }, longEncoder).coalesce(1);
        
        //Map the node in nodeDS to filter the illegal node 
        Dataset<NodeEntry> wayNodes = nodeDS.joinWith(wayNodeIds.distinct(), 
        		wayNodeIds.distinct().col("value").equalTo(nodeDS.col("nodeId")))
        		.map(wn -> wn._1, nodeEncoder).cache();
        
        //Find intersections
        //If the node exist in at least two ways, the nodes will be regard as an intersection
        Dataset<Long> intersectionNodes = wayNodeIds.groupBy("value").count()
        		.filter("count >= 2").select("value").as(longEncoder);
        Broadcast<List<Long>> broadcastInters = sc.broadcast(intersectionNodes.toJavaRDD().collect());
        
        //Label all the legal node in way with the intersection.
        //If the node is in the intersection, labeled it with true otherwise false
        //wayid, list(nodeid, isintersection)
		Dataset<LabeledWay> labeledWays = wayDS.map(w ->{
        		ArrayList<Tuple2<Long, Boolean>> nodesWithLabels = Lists.newArrayList(
        				w.getNodes()
        					.stream()
        					.map(id -> new Tuple2<Long, Boolean>(id, broadcastInters.getValue().contains(id))).iterator()
        				);
        		//label the begin node and end node in the way with	true	
        		nodesWithLabels.set(nodesWithLabels.size()-1, 
        				new Tuple2<Long, Boolean>(nodesWithLabels.get(nodesWithLabels.size()-1)._1, true));
        		nodesWithLabels.set(0, new Tuple2<Long, Boolean>(nodesWithLabels.get(0)._1, true));
        		return new LabeledWay(w.getWayId(), nodesWithLabels);
        }, lwEncoder);
        
		//Map the way to segments by intersection node
        JavaPairRDD<Long, ArrayList<Tuple3<Long, ArrayList<Long>, ArrayList<Long>>>> segmentedWays = labeledWays.javaRDD().mapToPair(lw ->{
        	return new Tuple2<Long, ArrayList<Tuple3<Long, ArrayList<Long>, ArrayList<Long>>>>(
        			lw.getWayId(), 
        			new MakeSegments(lw.getLabeledNodes()).getTupleList());
        });
        
        Dataset<Tuple2<Long, Intersection>> segmentWaysDS = labeledWays.flatMap(lw ->{
        		ArrayList<Intersection> sws = new MakeSegments(lw.getLabeledNodes()).getIntersectList();
        		return sws.stream().map(intersect -> new Tuple2<Long, Intersection>(lw.getWayId(),intersect)).iterator();
        }, tupleEncoder);
        
        //(intersectionId, Map(WayId, (inBuffer, outBuffer)))
        JavaPairRDD<Object, Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>>> intersectVertices = segmentWaysDS.toJavaRDD().mapToPair(sw -> {
    			Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>> map = new HashMap<>();
    			map.put(sw._1, new Tuple2<>(Lists.newArrayList(sw._2.getInBuf()), Lists.newArrayList(sw._2.getOutBuf())));
    			return new Tuple2<>((Object)sw._2.getOSMId(), map);
        }).reduceByKey((a, b) -> {
        		a.putAll(b);
        		return a;
        });
        
        //Edge(long srcId, long dstId, ED attr) 
        //Edge(long srcIntersectionId, long destIntersectionId, ED wayId) 
        JavaRDD<Edge<Long>> edges = segmentedWays.filter(way -> way._2.size() > 1).flatMap(way -> {
        	return new SlidingList<Tuple3<Long, ArrayList<Long>, ArrayList<Long>>>(way._2).windows(2).stream().flatMap(segment ->{
        		return Lists.newArrayList(Arrays.asList(
        				new Edge<>(segment._1._1(), segment._2._1(), way._1),
        				new Edge<>(segment._2._1(), segment._1._1(), way._1)
        				)).stream();
        		}).collect(Collectors.toList()).iterator();
        });
        
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
        

        java.util.Map<Long, Tuple2<Double, Double>> OSMNodes = wayNodes.javaRDD().mapToPair(node ->{
        		return new Tuple2<>(node.getNodeId(), new Tuple2<>(node.getLat(), node.getLon()));
        }).collectAsMap();
        
        //transform edge from Edge(long srcIntersectionId, long destIntersectionId, ED wayId) 
        // to Edge(long srcIntersectionId, long destIntersectionId, ED <wayId, distance(srcIntersectionId, destIntersectionId)>) 
        Graph<Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>>, Tuple2<Long, Double>> weightedRoadGraph = roadGraph.mapTriplets(
        		new AbsDistFunc(OSMNodes), ClassTag$.MODULE$.apply(Tuple2.class));
        
//        weightedRoadGraph.vertices().toJavaRDD().take(10).forEach(r ->{
//        	System.out.println(r);
//        });
        
        //32884939,32884943
        Seq<Long> request = convertListToSeq(Arrays.asList(444505024L,559552998L));
        
        //Seq<Object> request = convertListToSeqObj(Arrays.asList(444505024L,559552998L));
        
//        Graph<Tuple2<Double, ArrayList<Long>>, Tuple2<Long, Double>> initalGraph = weightedRoadGraph.mapVertices(
//        		new SerializableFunction2(request), ClassTag$.MODULE$.apply(Tuple2.class), eqMap);
        
        //vertices (intersectionId, (distance, intersetion legs))
        Graph<Tuple2<Double, ArrayList<Long>>, Tuple2<Long, Double>> initalGraph = weightedRoadGraph.mapVertices(
        		new SerializableFunction2(request), ClassTag$.MODULE$.apply(Tuple2.class), eqMap);
//        GraphOps ops = new GraphOps(initalGraph, ClassTag$.MODULE$.apply(Tuple2.class), ClassTag$.MODULE$.apply(Tuple2.class));
//        
//        ops.pregel(new Tuple2<Double, ArrayList<Long>>(Double.POSITIVE_INFINITY, new ArrayList<Long>()),
//        		Integer.MAX_VALUE,
//        		EdgeDirection.Out(),
//        		new Vprog(), 
//        		new SendMsg(),
//        		new MergeMsg(),
//        		ClassTag$.MODULE$.apply(Tuple2.class));
        
		Pregel.apply(initalGraph, 
				new Tuple2<Double, ArrayList<Long>>(Double.POSITIVE_INFINITY, new ArrayList<Long>()), 
				Integer.MAX_VALUE, 
				EdgeDirection.Out(), 
				new Vprog(), 
				new SendMsg(), 
				new MergeMsg(), 
				ClassTag$.MODULE$.apply(Tuple2.class), 
				ClassTag$.MODULE$.apply(Tuple2.class), 
				ClassTag$.MODULE$.apply(Tuple2.class));
        spark.stop();
        
	}
	
	public static Seq<Long> convertListToSeq(List<Long> inputList) {
	    return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}
	
	public static Seq<Object> convertListToSeqObj(List<Object> inputList) {
	    return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}
	

	
	
}
