package com.zishanfu.sparkdemo.serializable;

import java.io.Serializable;
import java.util.ArrayList;

import scala.Tuple2;
import scala.runtime.AbstractFunction3;

//<Object,VD,A,VD>
//<IntersectionId, VD, A, VD>
//A message is the final cost and final result for source and destination
public class Vprog extends AbstractFunction3<Object,Tuple2<Double, ArrayList<Long>>, Tuple2<Double, ArrayList<Long>>, Tuple2<Double, ArrayList<Long>>> implements Serializable {
	private static final long serialVersionUID = -5847419985842546413L;

	public Vprog() {}

	@Override
	public Tuple2<Double, ArrayList<Long>> apply(Object intersectionId, Tuple2<Double, ArrayList<Long>> verticeValue,
			Tuple2<Double, ArrayList<Long>> message) {
        if (message._1 == Double.POSITIVE_INFINITY) {    // superstep 0      
            return verticeValue;
        } else {
            if(verticeValue._1.compareTo(message._1) < 0) {
            	return verticeValue;
            }else {
            	return message;
            }
        }
	}



}
