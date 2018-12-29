package com.zishanfu.sparkdemo.entity;

import java.io.Serializable;
import java.util.ArrayList;

public class Intersection implements Serializable{
	private long OSMId;
	private ArrayList<Long> inBuf;
	private ArrayList<Long> outBuf;
	public Intersection() {}
	public Intersection(long OSMId, ArrayList<Long> inBuf, ArrayList<Long> outBuf) {
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
	public ArrayList<Long> getInBuf() {
		return inBuf;
	}
	public void setInBuf(ArrayList<Long> inBuf) {
		this.inBuf = inBuf;
	}
	public ArrayList<Long> getOutBuf() {
		return outBuf;
	}
	public void setOutBuf(ArrayList<Long> outBuf) {
		this.outBuf = outBuf;
	}
	
}
