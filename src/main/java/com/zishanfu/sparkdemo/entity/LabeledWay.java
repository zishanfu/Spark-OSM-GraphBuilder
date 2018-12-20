package com.zishanfu.sparkdemo.entity;

import java.io.Serializable;
import java.util.List;

import scala.Tuple2;

//List<Tuple2<Long, Boolean>>
public class LabeledWay implements Serializable{
	private long wayId;
	private List<Tuple2<Long, Boolean>> labeledNodes;
	public LabeledWay() {}
	public LabeledWay(long wayId, List<Tuple2<Long, Boolean>> labeledNodes) {
		this.wayId = wayId;
		this.labeledNodes = labeledNodes;
	}
	public long getWayId() {
		return wayId;
	}
	public void setWayId(long wayId) {
		this.wayId = wayId;
	}
	public List<Tuple2<Long, Boolean>> getLabeledNodes() {
		return labeledNodes;
	}
	public void setLabeledNodes(List<Tuple2<Long, Boolean>> labeledNodes) {
		this.labeledNodes = labeledNodes;
	}
	
}
