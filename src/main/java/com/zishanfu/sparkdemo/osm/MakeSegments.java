package com.zishanfu.sparkdemo.osm;

import java.io.Serializable;
import java.util.ArrayList;

import com.google.common.collect.Lists;
import com.zishanfu.sparkdemo.entity.*;

import scala.Tuple2;
import scala.Tuple3;

public class MakeSegments implements Serializable{
	private ArrayList<Intersection> intersectList = new ArrayList<>();
	private ArrayList<Tuple3<Long, ArrayList<Long>, ArrayList<Long>>> TupleList = new ArrayList<>();
	public MakeSegments() {}
	public MakeSegments(ArrayList<Tuple2<Long, Boolean>> way){
		//non intersection nodes list
		ArrayList<Long> segmentBuffer = new ArrayList<Long>();
		
		//One node in the way
		if(way.size() == 1) {
			Intersection intersect = new Intersection(way.get(0)._1, Lists.newArrayList(-1L), Lists.newArrayList(-1L));
			//return new ArrayList<>( Arrays.asList(intersect));
			intersectList.add(intersect);
			TupleList.add(new Tuple3<>(
					intersect.getOSMId(), 
					intersect.getInBuf(), 
					intersect.getOutBuf()));
		}else {

			for(int i = 0; i<way.size(); i++) {
				Tuple2<Long, Boolean> node = way.get(i);
				//node is intersection node
				if(node._2) {
					//If the node is in intersection, it means the node exist at least twice in way dataset
					Intersection newIntersect = new Intersection(node._1, new ArrayList<>(segmentBuffer), new ArrayList<>());
					intersectList.add(newIntersect);
					segmentBuffer.clear();
				}else {
					segmentBuffer.add(node._1);
				}
				
				//last node is false
				if(i == way.size() - 1 && !segmentBuffer.isEmpty()) {
					//add node to out buffer since the the node list is not intersection
					if(intersectList.isEmpty()) {
						intersectList.add(new Intersection(-1L, new ArrayList<Long>(), segmentBuffer));
					//add node to last intersection node's out buffer list
					}else {
						int last = intersectList.size() - 1;
						ArrayList<Long> of = intersectList.get(last).getOutBuf();
						of.addAll(segmentBuffer);
						intersectList.set(last, new Intersection(
								intersectList.get(last).getOSMId(),
								intersectList.get(last).getInBuf(),
								of));
					}
					segmentBuffer.clear();
				}
			}
			TupleList = Lists.newArrayList(intersectList.stream().map(i -> new Tuple3<Long, ArrayList<Long>, ArrayList<Long>>(i.getOSMId(), i.getInBuf(), i.getOutBuf())).iterator());
		}
		
		//return intersections;
	}

	public ArrayList<Intersection> getIntersectList() {
		return intersectList;
	}

	public ArrayList<Tuple3<Long, ArrayList<Long>, ArrayList<Long>>> getTupleList() {
		return TupleList;
	}
	
	
}
