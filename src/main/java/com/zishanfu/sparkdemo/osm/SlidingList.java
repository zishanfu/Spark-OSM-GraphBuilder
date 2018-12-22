package com.zishanfu.sparkdemo.osm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

public class SlidingList<T> implements Serializable{
	private List<T> list;
	public SlidingList() {}
	public SlidingList(List<T> list) {
		this.list = list;
	}
	
	/**
	 * @param width with minimum window size 2
	 * @return window list
	 */
	public List<Tuple2<T, T>> windows(int width){
		
		if(width < 2) {
			return null;
		}
		
		List<Tuple2<T, T>> res = new ArrayList<Tuple2<T, T>>();
		for(int i = width - 1; i<list.size(); i++) {
			T t1 = list.get(i - width + 1);
			T t2 = list.get(i);
			res.add(new Tuple2<>(t1, t2));
		}
		return res;
	}
}
