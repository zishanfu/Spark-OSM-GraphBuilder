package com.zishanfu.sparkdemo.serializable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;
import scala.runtime.AbstractFunction2;

public class MergeMsg extends AbstractFunction2<Tuple2<Double, ArrayList<Long>>, Tuple2<Double, ArrayList<Long>>, Tuple2<Double, ArrayList<Long>>> implements Serializable{
	
	public MergeMsg() {}

	@Override
	public Tuple2<Double, ArrayList<Long>> apply(Tuple2<Double, ArrayList<Long>> a, Tuple2<Double, ArrayList<Long>> b) {
		System.out.println("MergeMsg");
        if(a._1.compareTo(b._1) < 0) return a;
        return b;
	}
	


}
