package gash.router.server;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeList;
import gash.router.server.edges.EdgeMonitor;
import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

public class NodeChannelManager {
	protected static Logger logger = LoggerFactory.getLogger("NodeChannelManager");

	protected static AtomicReference<NodeChannelManager> instance = new AtomicReference<NodeChannelManager>();

	public static ConcurrentHashMap<Integer, Channel> node2ChannelMap = new ConcurrentHashMap<Integer, Channel>();
	public static ConcurrentHashMap<CommandMessage, Channel> clientChannelMap = new ConcurrentHashMap<CommandMessage, Channel>();
	private static int delay = 3000;

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

	public static ConcurrentHashMap<Integer, Channel> getNode2ChannelMap() {
		return node2ChannelMap;
	}

	// Returns next available channel for taking in read requests
	public static Channel getNextReadChannel() {
		// TODO
		return null;
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
						for (Integer nodeId : keySet2) {
							if (nodeId != null && !node2ChannelMap.containsKey(nodeId)) {
								node2ChannelMap.put(nodeId, edgeListMap.get(nodeId).getChannel());
							}
						}
					}
				}
				if (outboundEdges != null) {
					ConcurrentHashMap<Integer, EdgeInfo> edgeListMap = outboundEdges.getEdgeListMap();
					if (edgeListMap != null && !edgeListMap.isEmpty()) {
						Set<Integer> keySet2 = edgeListMap.keySet();
						for (Integer nodeId : keySet2) {
							if (nodeId != null && !node2ChannelMap.containsKey(nodeId)) {
								node2ChannelMap.put(nodeId, edgeListMap.get(nodeId).getChannel());
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
	 * @author savio
	 * @param message
	 * @param channel
	 */
	public static void addClientToMap(CommandMessage message, Channel channel){
		clientChannelMap.put(message, channel);
	}
	
	/**
	 * @author savio
	 * @param client
	 * @param filename
	 * @return
	 */
	public static Channel getClientChannelFromMap(String client, String filename){
		
		//TODO Problem : If the client has sent multiple requests for the same filename - what do we do?
		for (CommandMessage msg : clientChannelMap.keySet()) {
			if(msg.getTask().getFilename().equals(filename) && msg.getTask().getSender().equals(client))
			{
				return clientChannelMap.remove(msg);
			}
		}
		
		return null;
	}
}
