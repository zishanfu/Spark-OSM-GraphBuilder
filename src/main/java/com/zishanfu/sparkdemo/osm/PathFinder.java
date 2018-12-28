package com.zishanfu.sparkdemo.osm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PathFinder {
	static Map<Long, String> labels = ImmutableMap.<Long, String>builder()
            .put(1l, "A")
            .put(2l, "B")
            .put(3l, "C")
            .put(4l, "D")
            .put(5l, "E")
            .put(6l, "F")
            .build();

    private static class VProg extends AbstractFunction3<Long,Tuple2<Integer, ArrayList<String>>, Tuple2<Integer, ArrayList<String>> ,Tuple2<Integer, ArrayList<String>>> implements Serializable {
        @Override
        public Tuple2<Integer, ArrayList<String>> apply(Long vertexID, Tuple2<Integer, ArrayList<String>> vertexValue, Tuple2<Integer, ArrayList<String>> message) {
            if (message._1 == Integer.MAX_VALUE) {             // superstep 0
                return vertexValue;
            } else {
                // superstep > 0
                if(vertexValue._1.compareTo(message._1) < 0)
                    return vertexValue;
                return message;
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Tuple2<Integer, ArrayList<String>>,Integer>, Iterator<Tuple2<Object,Tuple2<Integer, ArrayList<String>>>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Tuple2<Integer, ArrayList<String>>>> apply(EdgeTriplet<Tuple2<Integer, ArrayList<String>>, Integer> triplet) {
            Tuple2<Object, Tuple2<Integer, ArrayList<String>>> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object, Tuple2<Integer, ArrayList<String>>> dstVertex = triplet.toTuple()._2();
            Integer weight = triplet.toTuple()._3();

            if (sourceVertex._2._1 + weight > dstVertex._2._1 || sourceVertex._2._1 == Integer.MAX_VALUE) {
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Tuple2<Integer, ArrayList<String>>>>().iterator()).asScala();
            } else {
                ArrayList<String> list = (ArrayList<String>) sourceVertex._2._2.clone();
                list.add(labels.get(dstVertex._1));
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object, Tuple2<Integer, ArrayList<String>>>(triplet.dstId(), new Tuple2<Integer, ArrayList<String>>(sourceVertex._2._1 + weight, list))).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Tuple2<Integer, ArrayList<String>>,Tuple2<Integer, ArrayList<String>>,Tuple2<Integer, ArrayList<String>>> implements Serializable {
        @Override
        public Tuple2<Integer, ArrayList<String>> apply(Tuple2<Integer, ArrayList<String>> o, Tuple2<Integer, ArrayList<String>> o2) {
            if(o._1.compareTo(o2._1) < 0) return o;
            return o2;
        }
    }

    public static void main(String[] agrs) {
    		SparkSession spark = SparkSession
	    		  .builder()
	    		  .master("local")
	    		  .appName("OSMParser")
	    		  .getOrCreate();
		JavaSparkContext ctx = new JavaSparkContext(spark.sparkContext());
		
        ArrayList<String> list = new ArrayList<String>();
        list.add(labels.get(1l));
        List<Tuple2<Object, Tuple2<Integer, ArrayList<String>>>> vertices = Lists.newArrayList(
                new Tuple2<Object,Tuple2<Integer, ArrayList<String>>>(1l, new Tuple2<>(0, list)),
                new Tuple2<Object,Tuple2<Integer, ArrayList<String>>>(2l, new Tuple2<>(Integer.MAX_VALUE, new ArrayList<String>())),
                new Tuple2<Object,Tuple2<Integer, ArrayList<String>>>(3l, new Tuple2<>(Integer.MAX_VALUE, new ArrayList<String>())),
                new Tuple2<Object,Tuple2<Integer, ArrayList<String>>>(4l, new Tuple2<>(Integer.MAX_VALUE, new ArrayList<String>())),
                new Tuple2<Object,Tuple2<Integer, ArrayList<String>>>(5l, new Tuple2<>(Integer.MAX_VALUE, new ArrayList<String>())),
                new Tuple2<Object,Tuple2<Integer, ArrayList<String>>>(6l, new Tuple2<>(Integer.MAX_VALUE, new ArrayList<String>()))
        );

        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object, Tuple2<Integer, ArrayList<String>>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);


        Graph<Tuple2<Integer, ArrayList<String>>,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new Tuple2<Integer, ArrayList<String>>(1, new ArrayList<String>()), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));


        ops.pregel(
                new Tuple2<Integer, ArrayList<String>>(Integer.MAX_VALUE, new ArrayList<String>()),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                ClassTag$.MODULE$.apply(Tuple2.class))
                .vertices()
                .toJavaRDD()
                .sortBy(v -> { return labels.get(((Tuple2<Object, Tuple2<Integer, ArrayList<String>>>)v)._1); }, true, 1)
                .foreach(v -> {
                    Tuple2<Object,Tuple2<Integer, ArrayList<String>>> vertex = (Tuple2<Object,Tuple2<Integer, ArrayList<String>>>)v;
                    System.out.println("Minimum path to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is "+ vertex._2._2.toString()  +" with cost "+vertex._2._1 );
                });
    }

}
