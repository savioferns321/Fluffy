package gash.router.persistence;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.NodeChannelManager;
import gash.router.server.ServerState;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

public class DataReplicationManager {

	protected static Logger logger = LoggerFactory.getLogger("DataReplicationManager");
	protected static ServerState serverState;

	protected static AtomicReference<DataReplicationManager> instance = new AtomicReference<DataReplicationManager>();

	public static DataReplicationManager initNodeChannelManager(ServerState serverState) {
		DataReplicationManager.serverState = serverState;
		instance.compareAndSet(null, new DataReplicationManager());
		System.out.println(" --- Initializing Data Replication Manager --- ");
		return instance.get();
	}

	public static DataReplicationManager getInstance() {
		return instance.get();
	}

	//TODO convert ReplicationInfo to WorkMessage
	public void replicate(WorkMessage workmessage) {
		ConcurrentHashMap<Integer, Channel> node2ChannelMap = NodeChannelManager.getNode2ChannelMap();
		if (node2ChannelMap != null && !node2ChannelMap.isEmpty()) {

			Set<Integer> keySet2 = node2ChannelMap.keySet();
			for (Integer nodeId : keySet2) {
				Channel nodeChannel = node2ChannelMap.get(nodeId);
				Replication replication = new Replication(workmessage, nodeChannel);
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
		//private ReplicationInfo replicationInfo;
		private WorkMessage workmessage;
		private Channel nodeChannel;

		public Replication(WorkMessage workmessage, Channel nodeChannel) {
			this.workmessage = workmessage;
			this.nodeChannel = nodeChannel;
		}
	

		@Override
		public void run() {
			if (this.nodeChannel.isOpen() && this.nodeChannel.isActive()) {
				this.nodeChannel.writeAndFlush(workmessage);
			} else {
				logger.error("The nodeChannel to " + nodeChannel.localAddress() + " is not active");
			}
		}

	}
}


