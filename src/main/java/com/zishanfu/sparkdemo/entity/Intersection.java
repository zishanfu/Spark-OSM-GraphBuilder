package com.zishanfu.sparkdemo.entity;

import java.io.Serializable;
import java.util.List;

public class Intersection implements Serializable{
	private long OSMId;
	private List<Long> inBuf;
	private List<Long> outBuf;
	public Intersection() {}
	public Intersection(long OSMId, List<Long> inBuf, List<Long> outBuf) {
		this.OSMId = OSMId;
		this.inBuf = inBuf;
		this.outBuf = outBuf;
	}
	public long getOSMId() {
		return OSMId;
	}
	public void setOSMId(long oSMId) {
		OSMId = oSMId;
	}
	public List<Long> getInBuf() {
		return inBuf;
	}
	public void setInBuf(List<Long> inBuf) {
		this.inBuf = inBuf;
	}
	public List<Long> getOutBuf() {
		return outBuf;
	}
	public void setOutBuf(List<Long> outBuf) {
		this.outBuf = outBuf;
	}
	
}
