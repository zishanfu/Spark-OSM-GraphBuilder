package com.zishanfu.sparkdemo.entity;

import java.io.Serializable;
import java.util.*;

//nodeId: Long, latitude: Double, longitude: Double, tags: Array[String]
public class NodeEntry implements Serializable{
	private long nodeId;
	private double lat;
	private double lon;
	private List<String> tags;
	
	public NodeEntry() {}
	
	public NodeEntry(long nodeId, double lat, double lon, List<String> tags) {
		this.nodeId = nodeId;
		this.lat = lat;
		this.lon = lon;
		this.tags = tags;
	}
	
	public long getNodeId() {
		return nodeId;
	}
	public void setNodeId(long nodeId) {
		this.nodeId = nodeId;
	}
	public double getLat() {
		return lat;
	}
	public void setLat(double lat) {
		this.lat = lat;
	}
	public double getLon() {
		return lon;
	}
	public void setLon(double lon) {
		this.lon = lon;
	}
	public List<String> getTags() {
		return tags;
	}
	public void setTags(List<String> tags) {
		this.tags = tags;
	}
	
}
