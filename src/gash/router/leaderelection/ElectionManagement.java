package gash.router.leaderelection;

import java.util.concurrent.atomic.AtomicReference;

import gash.router.container.RoutingConf;
import gash.router.server.NodeChannelManager;

public class ElectionManagement {

	protected static AtomicReference<ElectionManagement> instance = new AtomicReference<ElectionManagement>();

	private static RoutingConf routingConf = null;

	private int nodeId;
	private int numberOfVotes;

	private static final int MINIMUM_NUMBER_OF_NODES_REQUIRED = 3;

	public ElectionManagement(int nodeId, int numberOfVotes) {
		this.nodeId = nodeId;
		this.numberOfVotes = numberOfVotes;
	}

	public ElectionManagement getInstance(int nodeId, int numberOfVotest) {
		instance.compareAndSet(null, new ElectionManagement(nodeId, numberOfVotest));
		return instance.get();
	}

	public static ElectionManagement initElectionManagement(RoutingConf routingConf) {
		ElectionManagement.routingConf = routingConf;
		instance.compareAndSet(null, new ElectionManagement(routingConf.getNodeId(), 1));
		return instance.get();
	}

	public static boolean isReadyForElection() {
		return NodeChannelManager.numberOfActiveChannels() >= MINIMUM_NUMBER_OF_NODES_REQUIRED ? true : false;
	}
	
	public void startElection(){
		
	}
	public int getNodeId() {
		return nodeId;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

	public int getNumberOfVotes() {
		return numberOfVotes;
	}

	public void setNumberOfVotes(int numberOfVotes) {
		this.numberOfVotes = numberOfVotes;
	}

}
