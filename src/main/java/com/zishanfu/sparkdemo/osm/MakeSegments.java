package com.zishanfu.sparkdemo.osm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.zishanfu.sparkdemo.entity.*;

import scala.Tuple2;
import scala.Tuple3;

public class MakeSegments implements Serializable{
	private List<Intersection> intersectList = new ArrayList<>();
	private List<Tuple3<Long, List<Long>, List<Long>>> TupleList = new ArrayList<>();
	public MakeSegments() {}
	public MakeSegments(List<Tuple2<Long, Boolean>> way){
		//List<Intersection> intersections = new ArrayList<Intersection>();
		List<Long> segmentBuffer = new ArrayList<Long>();
		
		if(way.size() == 1) {
			Intersection intersect = new Intersection(way.get(0)._1, new ArrayList<Long>(Arrays.asList(-1L)), new ArrayList<Long>(Arrays.asList(-1L)));
			//return new ArrayList<>( Arrays.asList(intersect));
			intersectList.add(intersect);
			TupleList.add(new Tuple3<>(
					intersect.getOSMId(), 
					intersect.getInBuf(), 
					intersect.getOutBuf()));
		}else {
			//Tuple2<Long, Boolean>
			//(id, isIntersection)
			for(int i = 0; i<way.size(); i++) {
				Tuple2<Long, Boolean> node = way.get(i);
				if(node._2) {
					Intersection newIntersect = new Intersection(node._1, new ArrayList<>(segmentBuffer), new ArrayList<>());
					intersectList.add(newIntersect);
					segmentBuffer.clear();
				}else {
					segmentBuffer.add(node._1);
				}
				
				if(i == way.size() - 1 && !segmentBuffer.isEmpty()) {
					if(intersectList.isEmpty()) {
						intersectList.add(new Intersection(-1L, new ArrayList<Long>(), segmentBuffer));
					}else {
						int last = intersectList.size() - 1;
						List<Long> of = intersectList.get(last).getOutBuf();
						of.addAll(segmentBuffer);
						intersectList.set(last, new Intersection(
								intersectList.get(last).getOSMId(),
								intersectList.get(last).getInBuf(),
								of));
					}
					segmentBuffer.clear();
				}
			}
			TupleList = intersectList.stream().map(i -> new Tuple3<Long, List<Long>, List<Long>>(i.getOSMId(), i.getInBuf(), i.getOutBuf())).collect(Collectors.toList());
		}
		
		//return intersections;
	}

	public List<Intersection> getIntersectList() {
		return intersectList;
	}

	public List<Tuple3<Long, List<Long>, List<Long>>> getTupleList() {
		return TupleList;
	}
	
	
}
