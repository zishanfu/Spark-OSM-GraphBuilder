package com.zishanfu.sparkdemo.serializable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.graphx.EdgeTriplet;

import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import com.google.common.collect.Lists;

//<EdgeTriplet<VD,ED>,scala.collection.Iterator<scala.Tuple2<Object,A>>>
public class SendMsg extends AbstractFunction1<EdgeTriplet<Tuple2<Double, ArrayList<Long>>, Tuple2<Long, Double>>,Iterator<Tuple2<Object,Tuple2<Double, ArrayList<Long>>>>> implements Serializable{
	
	public SendMsg() {}

	//<Object, A>
	//<IntersectionId, message>
	//EdgeTriplet<VD,ED>
	//VD <cost, node list>
	//ED <wayId, distance between intersection nodes>
	
//    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Tuple2<Integer, ArrayList<String>>,Integer>, Iterator<Tuple2<Object,Tuple2<Integer, ArrayList<String>>>>> implements Serializable {
//        @Override
//        public Iterator<Tuple2<Object, Tuple2<Integer, ArrayList<String>>>> apply(EdgeTriplet<Tuple2<Integer, ArrayList<String>>, Integer> triplet) {
//            Tuple2<Object, Tuple2<Integer, ArrayList<String>>> sourceVertex = triplet.toTuple()._1();
//            Tuple2<Object, Tuple2<Integer, ArrayList<String>>> dstVertex = triplet.toTuple()._2();
//            Integer weight = triplet.toTuple()._3();
//
//            if (sourceVertex._2._1 + weight > dstVertex._2._1 || sourceVertex._2._1 == Integer.MAX_VALUE) {
//                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Tuple2<Integer, ArrayList<String>>>>().iterator()).asScala();
//            } else {
//                ArrayList<String> list = (ArrayList<String>) sourceVertex._2._2.clone();
//                list.add(labels.get(dstVertex._1));
//                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object, Tuple2<Integer, ArrayList<String>>>(triplet.dstId(), new Tuple2<Integer, ArrayList<String>>(sourceVertex._2._1 + weight, list))).iterator()).asScala();
//            }
//        }
//    }
//    
	@Override
	public Iterator<Tuple2<Object, Tuple2<Double, ArrayList<Long>>>> apply(
			EdgeTriplet<Tuple2<Double, ArrayList<Long>>, Tuple2<Long, Double>> triplet) {
		//(<Object,VD>, <Object,VD>, ED)
		//(current vertices, neighboring vertices, edge)
		System.out.println("SendMsg");
		
        Tuple2<Object, Tuple2<Double, ArrayList<Long>>> sourceVertex = triplet.toTuple()._1();
        Tuple2<Object, Tuple2<Double, ArrayList<Long>>> dstVertex = triplet.toTuple()._2();
        double weight = triplet.toTuple()._3()._2();
        
        if (sourceVertex._2._1 + weight > dstVertex._2._1 || sourceVertex._2._1 == Double.POSITIVE_INFINITY) {
            return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Tuple2<Double, ArrayList<Long>>>>().iterator()).asScala();
        } else {
            ArrayList<Long> list = (ArrayList<Long>) sourceVertex._2()._2().clone();
            list.add((long) dstVertex._1);
            return JavaConverters.asScalaIteratorConverter(
            		Arrays.asList(
            				new Tuple2<Object, Tuple2<Double, ArrayList<Long>>>(
            						triplet.dstId(), 
            						new Tuple2<Double, ArrayList<Long>>(sourceVertex._2._1 + weight, list)
            						)
            				).iterator()).asScala();
        }
	}



}
