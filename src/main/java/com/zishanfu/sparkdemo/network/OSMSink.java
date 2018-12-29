package com.zishanfu.sparkdemo.network;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.pbf2.v0_6.PbfReader;

import com.zishanfu.sparkdemo.entity.NodeEntry;
import com.zishanfu.sparkdemo.entity.WayEntry;

import scala.collection.mutable.ArrayBuffer;

public class OSMSink {
	private ArrayBuffer<NodeEntry> nodes;
	private ArrayBuffer<WayEntry> ways;
	
	public OSMSink() {
		this.nodes = new ArrayBuffer<>();
		this.ways = new ArrayBuffer<>();
	}
	
	public void run(String osmpath) {
		//Digest OSM data
		
		Set<String> allowableWays = new HashSet<>(Arrays.asList(  
				  "motorway",
				  "motorway_link",
				  "trunk",
				  "trunk_link",
				  "primary",
				  "primary_link",
				  "secondary",
				  "secondary_link",
				  "tertiary",
				  "tertiary_link",
				  "living_street",
				  "residential",
				  "road",
				  "construction",
				  "motorway_junction"));
		
		
		File osmFile = new File(osmpath);
		PbfReader reader = new PbfReader(osmFile, 1);

		 
		Sink sinkImplementation = new Sink() {
		 
			public void process(EntityContainer entityContainer) {
				Entity entity = entityContainer.getEntity();
			    List<String> tags = entity.getTags().stream().map(Tag::getValue).collect(Collectors.toList());
			    
			    if (entity instanceof Node) {
			    	nodes.$plus$eq(new NodeEntry(
			    			entity.getId(), 
			    			((Node) entity).getLatitude(),
			    			((Node) entity).getLongitude(), 
			    			tags));
			    } else if (entity instanceof Way) {
			    	
			    	Set<String> tagSet = entity.getTags().stream().map(Tag::getValue).collect(Collectors.toSet());
			    	//check if the way is allowable way
			    	if(tagSet.retainAll(allowableWays) && tagSet.size() != 0) {
			    		ways.$plus$eq(new WayEntry(
			    				entity.getId(), 
			    				tags, 
			    				((Way) entity).getWayNodes().stream().map(WayNode::getNodeId).collect(Collectors.toList())));
			    	}
			    }
			 }


			@Override
			public void complete() {}

			@Override
			public void release() {}

			@Override
			public void initialize(java.util.Map<String, Object> metaData) {}
		};
		 
		reader.setSink(sinkImplementation);
		reader.run();		
	}

	public ArrayBuffer<NodeEntry> getNodes() {
		return nodes;
	}

	public ArrayBuffer<WayEntry> getWays() {
		return ways;
	}
	
	
}
