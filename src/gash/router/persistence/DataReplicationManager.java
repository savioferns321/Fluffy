package gash.router.persistence;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.NodeChannelManager;
import gash.router.server.QueueManager;
import gash.server.util.MessageGeneratorUtil;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class DataReplicationManager {

	protected static Logger logger = LoggerFactory.getLogger("DataReplicationManager");

	protected static AtomicReference<DataReplicationManager> instance = new AtomicReference<DataReplicationManager>();

	public static DataReplicationManager initDataReplicationManager() {
		instance.compareAndSet(null, new DataReplicationManager());
		System.out.println(" --- Initializing Data Replication Manager --- ");
		return instance.get();
	}

	public static DataReplicationManager getInstance() throws Exception {
		if(instance!= null && instance.get()!= null){
			return instance.get();
		}
		throw new Exception(" Data Replication Manager not started ");
	}
/*
	//TODO convert ReplicationInfo to WorkMessage
	public void replicate(WorkMessage workmessage) {
		ConcurrentHashMap<Integer, Channel> node2ChannelMap = NodeChannelManager.getNode2ChannelMap();
		if (node2ChannelMap != null && !node2ChannelMap.isEmpty()) {

			Set<Integer> keySet2 = node2ChannelMap.keySet();
			for (Integer nodeId : keySet2) {
				Channel nodeChannel = node2ChannelMap.get(nodeId);
				Replication replication = new Replication(workmessage, nodeChannel, nodeId);
				Thread replicationThread = new Thread(replication);
				replicationThread.start();
			}

		}
	}
	*/
	public void replicate(CommandMessage message) {
		ConcurrentHashMap<Integer, Channel> node2ChannelMap = NodeChannelManager.getNode2ChannelMap();
		if (node2ChannelMap != null && !node2ChannelMap.isEmpty()) {

			Set<Integer> keySet2 = node2ChannelMap.keySet();
			for (Integer nodeId : keySet2) {
				Channel nodeChannel = node2ChannelMap.get(nodeId);
				Replication replication = new Replication(message, nodeChannel, nodeId);
				Thread replicationThread = new Thread(replication);
				replicationThread.start();
			}

		}
	}

	//TODO Remove work message and pass only data  
	//TODO check if work message hasreplication()
	/*public boolean writeToDB(WorkMessage workmessage){
		
			String fileName = workmessage.getFileName();
			byte[] fileContent = workmessage.getFileContent().tobytearray();
			Dbhandler dbHandler = new Dbhandler();
			if(!dbHandler.addFile(fileName, fileContent)){
				return false;
			}else{
				return true;
			
	}
}*/

	
	// TODO convert this to Future and Callable
	private class Replication implements Runnable {
		private CommandMessage commandMessage;
		private Channel nodeChannel;
		private int nodeId;

		public Replication(CommandMessage commandMessage, Channel nodeChannel, Integer nodeId) {
			//Command Message here contains the chunk ID and the chunk nos. and the chunk byte, in its Task field 
			this.commandMessage = commandMessage;
			this.nodeChannel = nodeChannel;
			this.nodeId = nodeId;
		}


		@Override
		public void run() {
			if (this.nodeChannel.isOpen() && this.nodeChannel.isActive()) {
				//this.nodeChannel.writeAndFlush(replicationInfo);
				
				//Generate the work message to send to the slave nodes
				WorkMessage workMsg = MessageGeneratorUtil.getInstance().generateReplicationReqMsg(commandMessage, nodeId);
				
				//Push this message to the outbound work queue 
				try {
					logger.info("Sending message for replication ");
					QueueManager.getInstance().enqueueOutboundWork(workMsg, nodeChannel);
				} catch (Exception e) {
					logger.error(e.getMessage());
					e.printStackTrace();
				}
			} else {
				logger.error("The nodeChannel to " + nodeChannel.localAddress() + " is not active");
			}
		}

	}
}


