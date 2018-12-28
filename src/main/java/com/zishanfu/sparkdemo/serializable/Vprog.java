package com.zishanfu.sparkdemo.serializable;

import java.io.Serializable;
import java.util.ArrayList;

import scala.Tuple2;
import scala.runtime.AbstractFunction3;

public class Vprog extends AbstractFunction3<Object,Tuple2<Double, ArrayList<Long>>, Tuple2<Double, ArrayList<Long>>, Tuple2<Double, ArrayList<Long>>> implements Serializable {
	private static final long serialVersionUID = -5847419985842546413L;

	public Vprog() {}

	@Override
	public Tuple2<Double, ArrayList<Long>> apply(Object vertexID, Tuple2<Double, ArrayList<Long>> vertexValue,
			Tuple2<Double, ArrayList<Long>> message) {
        if (message._1 == Double.POSITIVE_INFINITY) {    // superstep 0      
            return vertexValue;
        } else {
            // superstep > 0
            if(vertexValue._1.compareTo(message._1) < 0)
                return vertexValue;
            return message;
        }
	}



}
