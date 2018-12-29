package com.zishanfu.sparkdemo.entity;

import java.io.Serializable;
import java.util.ArrayList;
import scala.Tuple2;

//List<Tuple2<Long, Boolean>>
public class LabeledWay implements Serializable{
	private long wayId;
	private ArrayList<Tuple2<Long, Boolean>> labeledNodes;
	public LabeledWay() {}
	public LabeledWay(long wayId, ArrayList<Tuple2<Long, Boolean>> labeledNodes) {
		this.wayId = wayId;
		this.labeledNodes = labeledNodes;
	}
	public long getWayId() {
		return wayId;
	}
	public void setWayId(long wayId) {
		this.wayId = wayId;
	}
	public ArrayList<Tuple2<Long, Boolean>> getLabeledNodes() {
		return labeledNodes;
	}
	public void setLabeledNodes(ArrayList<Tuple2<Long, Boolean>> labeledNodes) {
		this.labeledNodes = labeledNodes;
	}
	
}
