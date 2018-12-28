package com.zishanfu.sparkdemo.serializable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

import scala.Tuple2;
import scala.collection.Seq;
import scala.runtime.AbstractFunction2;
import com.google.common.collect.Lists;

public class SerializableFunction2 extends AbstractFunction2<Object, Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>>, Tuple2<Double, ArrayList<Long>>> implements Serializable {

	private static final long serialVersionUID = -2505347502975782585L;
	private Seq<Long> request;
	
	public SerializableFunction2(Seq<Long> request) {
		System.out.println("sf2");
		this.request = request;
	}
	
	@Override
	public Tuple2<Double, ArrayList<Long>> apply(Object a, Map<Long, Tuple2<ArrayList<Long>, ArrayList<Long>>> b) {
		if(b.containsKey(request.apply(0))) {
			System.out.println("contains");
			return new Tuple2<Double, ArrayList<Long>>(0.0, Lists.newArrayList());
		}else {
			return new Tuple2<Double, ArrayList<Long>>(Double.POSITIVE_INFINITY, Lists.newArrayList());
		}
	}

}
