package com.zishanfu.sparkdemo.serializable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import scala.Tuple2;
import scala.collection.Seq;
import scala.runtime.AbstractFunction2;

public class SerializableFunction2 extends AbstractFunction2<Object, Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>>, Tuple2<Double, ArrayList<Long>>> implements Serializable {

	private static final long serialVersionUID = -2505347502975782585L;
	private long srcNode;
	
	public SerializableFunction2(Seq<Long> request) {
		this.srcNode = request.apply(0);
		
	}
	
	@Override
	public Tuple2<Double, ArrayList<Long>> apply(Object intersectionId, Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>> waySegments) {
		ArrayList<Long> list = new ArrayList<Long>();
		if((long)intersectionId == srcNode || waySegments.containsKey(srcNode)) {
			list.add(srcNode);
			return new Tuple2<Double, ArrayList<Long>>(0.0, list);
		}else {
			return new Tuple2<Double, ArrayList<Long>>(Double.POSITIVE_INFINITY, list);
		}
	}

}
