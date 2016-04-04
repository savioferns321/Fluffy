package gash.router.server;

import java.util.Collection;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.QueueManager.CommandMessageChannelCombo;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeList;
import gash.router.server.edges.EdgeMonitor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import pipe.work.Work.WorkMessage;

public class NodeChannelManager {
	protected static Logger logger = LoggerFactory.getLogger("NodeChannelManager");

	protected static AtomicReference<NodeChannelManager> instance = new AtomicReference<NodeChannelManager>();

	public static ConcurrentHashMap<Integer, Channel> node2ChannelMap = new ConcurrentHashMap<Integer, Channel>();
	public static ConcurrentHashMap<String, CommandMessageChannelCombo> clientChannelMap = new ConcurrentHashMap<String, CommandMessageChannelCombo>();
	private static Queue<Integer> roundRobinQ = new LinkedBlockingQueue<Integer>();
	private static Queue<Integer> workStealQ = new LinkedBlockingQueue<Integer>();

	public static int currentLeaderID;
	public static String currentLeaderAddress;
	public static boolean amIPartOfNetwork = true;
	private static int delay = 3000;

	public static NodeChannelManager getInstance() {
		if (instance.get() == null)
			instance.compareAndSet(null, new NodeChannelManager());
		return instance.get();
	}

	public NodeChannelManager() {
		NodeMonitor nodeMonitor = new NodeMonitor();
		Thread thread = new Thread(nodeMonitor);
		thread.start();
	}

	public static NodeChannelManager initNodeChannelManager() {
		instance.compareAndSet(null, new NodeChannelManager());
		System.out.println(" --- Initializing Node Channel Manager -- ");
		return instance.get();
	}

	public static int numberOfActiveChannels() {
		return node2ChannelMap.size();
	}
	
	public static Channel getChannelByNodeId(int nodeId) {
		return node2ChannelMap.get(nodeId);
	}

	public static ConcurrentHashMap<Integer, Channel> getNode2ChannelMap() {
		return node2ChannelMap;
	}

	// Returns next available channel for taking in read requests
	public static Channel getNextReadChannel() {
		if (!roundRobinQ.isEmpty()) {
			Integer nodeId = roundRobinQ.remove();
			if(nodeId == currentLeaderID){
				roundRobinQ.add(nodeId);
				nodeId = roundRobinQ.remove();
			}
			if (node2ChannelMap.containsKey(nodeId)) {
				roundRobinQ.add(nodeId);
				return node2ChannelMap.get(nodeId);
			}
			roundRobinQ.add(nodeId);
		}
		logger.info("No channel found ");
		return null;
	}
	
	// Returns next available channel sending worksteal task
		public static Channel getNextChannelForSteal() {
			if (!workStealQ.isEmpty()) {
				Integer nodeId = workStealQ.remove();
				if(nodeId == currentLeaderID){
					workStealQ.add(nodeId);
					nodeId = workStealQ.remove();
				}
				if (node2ChannelMap.containsKey(nodeId)) {
					System.out.println("Node Channel was found");
					workStealQ.add(nodeId);
					return node2ChannelMap.get(nodeId);
				}
				workStealQ.add(nodeId);
			}
			logger.info("No channel found ");
			return null;
		}

	public static synchronized void broadcast(WorkMessage message) throws Exception {
		if (node2ChannelMap.isEmpty()) {
			System.out.println("----- No nodes are availble -----");
			return;
		}
		Collection<Channel> allChannel = node2ChannelMap.values();
		for (Channel channel : allChannel) {
			System.out.println("Sending message to Channel " + channel.toString());
			while(!channel.isWritable()){
				//Looping until channel is writable
			}			
			ChannelFuture cf = channel.writeAndFlush(message);
			if (cf.isDone() && !cf.isSuccess()) {
				logger.info("Failed to write the message to the channel ");
			}
		}
	}

	// To continuously check addition and removal of nodes to the current node
	private class NodeMonitor implements Runnable {
		private boolean forever = true;

		@Override
		public void run() {
			try {
				while (forever) {
					EdgeList inboundEdges = EdgeMonitor.getInboundEdges();
					EdgeList outboundEdges = EdgeMonitor.getOutboundEdges();
					addToNode2ChannelMap(inboundEdges, outboundEdges);
					System.out.println("node2Channel Map : " + node2ChannelMap.toString());
					// Make it efficient
					Thread.sleep(NodeChannelManager.delay);
				}

			} catch (InterruptedException e) {
				logger.error("An error has occured ", e);
			}
		}

		private void addToNode2ChannelMap(EdgeList inboundEdges, EdgeList outboundEdges) {
			try {

				if (inboundEdges != null) {
					ConcurrentHashMap<Integer, EdgeInfo> edgeListMap = inboundEdges.getEdgeListMap();
					if (edgeListMap != null && !edgeListMap.isEmpty()) {
						Set<Integer> keySet2 = edgeListMap.keySet();
						if (keySet2 != null)
							for (Integer nodeId : keySet2) {
								if (nodeId != null && !node2ChannelMap.containsKey(nodeId)
										&& edgeListMap.containsKey(nodeId)
										&& edgeListMap.get(nodeId).getChannel() != null) {
									logger.info("Added node " + nodeId + " " + edgeListMap.get(nodeId).getHost()
											+ " to channel map. ");
									node2ChannelMap.put(nodeId, edgeListMap.get(nodeId).getChannel());
									if (!roundRobinQ.contains(nodeId)) {
										roundRobinQ.add(nodeId);
									}
									if (!workStealQ.contains(nodeId)) {
										workStealQ.add(nodeId);
									}
								}
							}
					}
				}
				if (outboundEdges != null) {
					ConcurrentHashMap<Integer, EdgeInfo> edgeListMap = outboundEdges.getEdgeListMap();
					if (edgeListMap != null && !edgeListMap.isEmpty()) {
						Set<Integer> keySet2 = edgeListMap.keySet();
						if (keySet2 != null)
							for (Integer nodeId : keySet2) {
								if (nodeId != null && !node2ChannelMap.containsKey(nodeId)
										&& edgeListMap.containsKey(nodeId)
										&& edgeListMap.get(nodeId).getChannel() != null) {
									node2ChannelMap.put(nodeId, edgeListMap.get(nodeId).getChannel());
									if (!roundRobinQ.contains(nodeId)) {
										roundRobinQ.add(nodeId);
									}
									if (!workStealQ.contains(nodeId)) {
										workStealQ.add(nodeId);
									}
								}
							}
					}
				}

			} catch (Exception exception) {
				logger.error("An Error has occured ", exception);
			}
		}

	}

	/**
	 * Generates a UID String which serves as a key for storing a client channel
	 * in the server's map, while serving READ requests.
	 * 
	 * @author savio
	 * @param message
	 * @param channel
	 */
	public static String addClientToMap(CommandMessageChannelCombo combo) {
		UUID uuid = UUID.randomUUID();
		String uidString = uuid.toString();
		clientChannelMap.put(uidString, combo);
		return uidString;
	}

	/**
	 * Retrieves the client's channel from the stored map
	 * 
	 * @author savio
	 * @param client
	 * @param filename
	 * @return
	 */
	public static synchronized CommandMessageChannelCombo getClientChannelFromMap(String requestId) {

		if (clientChannelMap.containsKey(requestId) && clientChannelMap.get(requestId) != null) {
			return clientChannelMap.get(requestId);
		}
		logger.info("Unable to find the channel for request ID : " + requestId);
		return null;
	}

	/**
	 * Deletes the client's channel from the map.
	 * 
	 * @param requestId
	 * @throws Exception
	 */
	public static synchronized void removeClientChannelFromMap(String requestId) throws Exception {
		if (clientChannelMap.containsKey(requestId) && clientChannelMap.get(requestId) != null) {
			clientChannelMap.remove(requestId);
		} else {
			logger.error("Unable to find the channel for request ID : " + requestId);
			throw new Exception("Unable to find the node for this request ID : " + requestId);
		}

	}
}
