package com.zishanfu.sparkdemo.serializable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.graphx.EdgeTriplet;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import com.google.common.collect.Lists;

public class SendMsg extends AbstractFunction1<EdgeTriplet<Tuple2<Double, ArrayList<Long>>, Tuple2<Long, Double>>,Iterator<Tuple2<Object,Tuple2<Double, ArrayList<Long>>>>> implements Serializable{
	
	public SendMsg() {}

	@Override
	public Iterator<Tuple2<Object, Tuple2<Double, ArrayList<Long>>>> apply(
			EdgeTriplet<Tuple2<Double, ArrayList<Long>>, Tuple2<Long, Double>> triplet) {
        Tuple2<Object, Tuple2<Double, ArrayList<Long>>> sourceVertex = triplet.toTuple()._1();
        Tuple2<Object, Tuple2<Double, ArrayList<Long>>> dstVertex = triplet.toTuple()._2();
        double weight = triplet.toTuple()._3()._2();

        if (sourceVertex._2._1 + weight > dstVertex._2._1 || sourceVertex._2._1 == Double.POSITIVE_INFINITY) {
            return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Tuple2<Double, ArrayList<Long>>>>().iterator()).asScala();
        } else {
            ArrayList<Long> list = Lists.newArrayList(sourceVertex._2()._2());
            list.add((long) dstVertex._1);
            return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object, Tuple2<Double, ArrayList<Long>>>(triplet.dstId(), new Tuple2<Double, ArrayList<Long>>(sourceVertex._2._1 + weight, list))).iterator()).asScala();
        }
	}



}
