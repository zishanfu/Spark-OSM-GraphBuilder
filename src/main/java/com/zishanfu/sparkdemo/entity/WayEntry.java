package com.zishanfu.sparkdemo.entity;

import java.io.Serializable;
import java.util.*;

public class WayEntry implements Serializable{
	private long wayId;
	private List<String> tags;
	private List<Long> nodes;
	
	public WayEntry() {}
	
	public WayEntry(long wayId, List<String> tags, List<Long> nodes) {
		this.wayId = wayId;
		this.tags = tags;
		this.nodes = nodes;
	}
	
	public long getWayId() {
		return wayId;
	}
	public void setWayId(long wayId) {
		this.wayId = wayId;
	}
	public List<String> getTags() {
		return tags;
	}
	public void setTags(List<String> tags) {
		this.tags = tags;
	}
	public List<Long> getNodes() {
		return nodes;
	}
	public void setNodes(List<Long> nodes) {
		this.nodes = nodes;
	}

}
