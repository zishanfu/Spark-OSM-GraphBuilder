package com.zishanfu.sparkdemo.osm;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.spark.graphx.EdgeTriplet;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;

import scala.Tuple2;
import scala.runtime.AbstractFunction1;

public class AbsDistFunc extends AbstractFunction1<EdgeTriplet<Map<Long, Tuple2<List<Long>, List<Long>>>, Long>,Tuple2<Long, Double>> implements Serializable{
	
	private Map<Long, Tuple2<Double, Double>> OSMNodes;
	
	public AbsDistFunc(Map<Long, Tuple2<Double, Double>> OSMNodes) {
		this.OSMNodes = OSMNodes;
	}
	
	@Override
	public Tuple2<Long, Double> apply(EdgeTriplet<Map<Long, Tuple2<List<Long>, List<Long>>>, Long> triplet) {
		List<Long> wayNodes = triplet.dstAttr().get(triplet.attr)._1;
		if(wayNodes.isEmpty()) {
			return new Tuple2<>(triplet.attr, dist(triplet.srcId(), triplet.dstId()));
		}else {
			double distance = 0.0;
			distance += dist(triplet.srcId(), wayNodes.get(0));
			distance += dist(triplet.dstId(), wayNodes.get(wayNodes.size() - 1));
			if(wayNodes.size() > 1) {
				distance += new SlidingList<Long>(wayNodes).windows(2)
						.stream().map(buff ->dist(buff._1, buff._2)).reduce(.0, (a, b) -> a+b);
			}
			return new Tuple2<>(triplet.attr, distance);
		}
	}
	
	private double dist(long n1, long n2) {
		Tuple2<Double, Double> n1Coord = OSMNodes.get(n1);
		Tuple2<Double, Double> n2Coord = OSMNodes.get(n2);
		Point p1 = new Point(n1Coord._1, n1Coord._2);
		Point p2 = new Point(n2Coord._1, n2Coord._2);
		return GeometryEngine.geodesicDistanceOnWGS84(p1, p2);
	}
	
}
