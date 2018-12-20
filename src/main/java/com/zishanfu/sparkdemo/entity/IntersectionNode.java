package com.zishanfu.sparkdemo.entity;

import java.io.Serializable;
import java.util.List;

public class IntersectionNode implements Serializable{
	private long id;
	private List<Long> in;
	private List<Long> out;
	public IntersectionNode() {}
	public IntersectionNode(long id, List<Long> in, List<Long> out) {
		this.id = id;
		this.in = in;
		this.out = out;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public List<Long> getIn() {
		return in;
	}
	public void setIn(List<Long> in) {
		this.in = in;
	}
	public List<Long> getOut() {
		return out;
	}
	public void setOut(List<Long> out) {
		this.out = out;
	}
	
}
