package gash.server.util;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import gash.router.container.RoutingConf;
import gash.router.persistence.MessageDetails;
import gash.router.raft.leaderelection.NodeState;
import gash.router.server.MessageServer;
import gash.router.server.NodeChannelManager;
import gash.router.server.QueueManager;
import pipe.common.Common.Header;
import pipe.common.Common.Task;
import pipe.common.Common.Task.TaskType;
import pipe.monitor.Monitor.ClusterMonitor;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.StateOfLeader;
import pipe.work.Work.WorkMessage.Worktype;
import pipe.work.Work.WorkSteal;
import routing.Pipe.CommandMessage;

public class MessageGeneratorUtil {

	private static RoutingConf conf;

	protected static Logger logger = LoggerFactory.getLogger(MessageGeneratorUtil.class);
	protected static AtomicReference<MessageGeneratorUtil> instance = new AtomicReference<MessageGeneratorUtil>();

	public static MessageGeneratorUtil initGenerator() {
		instance.compareAndSet(null, new MessageGeneratorUtil());
		return instance.get();
	}

	private MessageGeneratorUtil() {

	}

	public static MessageGeneratorUtil getInstance(){
		return instance.get();
	}

	/**
	 * Leader sends this message to each node to replicate the data in the CommandMessage.
	 * @param message
	 * @param nodeId
	 * @return
	 */
	public WorkMessage generateReplicationReqMsg(CommandMessage message, Integer nodeId){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(nodeId);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);

		Task.Builder tb = Task.newBuilder(message.getTask());
		tb.setChunk(message.getTask().getChunk());
		wb.setTask(tb);
		//TODO Generate secret
		wb.setSecret(1234);
		wb.setIsProcessed(false);
		wb.setWorktype(Worktype.LEADER_WRITE);
		addLeaderFieldToWorkMessage(wb);

		return wb.build();
	}

	/**
	 * Leader sends this to client after it has finished processing write request.
	 * @param isSuccess
	 * @param nodeId
	 * @return
	 */
	public CommandMessage generateClientResponseMsg(boolean isSuccess){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(MessageServer.getNodeId());
		hb.setTime(System.currentTimeMillis());

		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		if(isSuccess)
			rb.setMessage(" File saved ");
		else
			rb.setMessage(" Operation Failed ");

		return rb.build();

	}

	/**
	 * Leader sends this to a slave node to service a READ request. 
	 * @param commandMessage
	 * @return
	 */
	public WorkMessage generateDelegationMessage(CommandMessage commandMessage, String requestID){

		Header.Builder hb = Header.newBuilder();
		//TODO Get node ID
		hb.setNodeId(MessageServer.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setTask(commandMessage.getTask());
		wb.setIsProcessed(false);
		//TODO Set the secret
		wb.setSecret(1234);
		wb.setRequestId(requestID);
		wb.setWorktype(Worktype.LEADER_READ);
		addLeaderFieldToWorkMessage(wb);

		return wb.build();
	}
	
	
	public WorkMessage generateStealMessage(){

		Header.Builder hb = Header.newBuilder();
		//TODO Get node ID
		hb.setNodeId(MessageServer.getNodeId());
		hb.setTime(System.currentTimeMillis());
		
		WorkSteal.Builder stealMessage = WorkSteal.newBuilder();
		stealMessage.setStealtype(pipe.work.Work.WorkSteal.StealType.STEAL_REQUEST);

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setIsProcessed(false);
		//TODO Set the secret
		wb.setSecret(1234);
		addLeaderFieldToWorkMessage(wb);
		wb.setSteal(stealMessage);
		
		return wb.build();
	}
	

	/**
	 * This message is sent from the slave to the leader when leader has requested a READ. A msg is generated for each file chunk.
	 * @return
	 */
	public WorkMessage generateDelegationRespMsg(Task t, byte[] currentByte, int chunkId, int totalChunks, String requestId){
		Header.Builder hb = Header.newBuilder();
		//TODO Get node ID
		hb.setNodeId(MessageServer.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		//TODO Set the secret
		wb.setSecret(1234);
		wb.setRequestId(requestId);
		wb.setWorktype(Worktype.SLAVE_READ_DONE);

		Task.Builder tb = Task.newBuilder();
		tb.setChunkNo(chunkId);
		tb.setChunk(ByteString.copyFrom(currentByte));
		tb.setNoOfChunks(totalChunks);
		tb.setTaskType(TaskType.READ);
		tb.setFilename(t.getFilename());
		tb.setSender(t.getSender());
		if(chunkId == totalChunks){
			wb.setIsProcessed(true);
		}else{
			wb.setIsProcessed(false);
		}

		wb.setTask(tb.build());
		addLeaderFieldToWorkMessage(wb);

		return wb.build();
	}

	/**
	 * Generates a Command Message from the Work Message sent by the slave node and sends it to the client.
	 * @param message
	 * @return
	 */
	public CommandMessage forwardChunkToClient(WorkMessage message){
		Header.Builder hb = Header.newBuilder();
		//TODO Get node ID
		hb.setNodeId(MessageServer.getNodeId());
		hb.setTime(System.currentTimeMillis());

		CommandMessage.Builder cb = CommandMessage.newBuilder();
		cb.setHeader(hb.build());
		cb.setTask(message.getTask());
		cb.setMessage("Success");


		return cb.build();
	}

	/**
	 * Sent from a slave node to the leader after it has completed replication. It sets the tasktype flag to SLAVE_WRITTEN.
	 * @param message
	 * @return
	 */
	public WorkMessage generateReplicationAckMessage(WorkMessage message){
		Header.Builder hb = Header.newBuilder();
		//TODO Get node ID
		hb.setNodeId(MessageServer.getNodeId());
		hb.setTime(System.currentTimeMillis());

		Task.Builder tb = Task.newBuilder(message.getTask());
		tb.clearChunk();
		tb.clearChunkNo();
		tb.clearNoOfChunks();

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setWorktype(Worktype.SLAVE_WRITTEN);
		//TODO Set the secret
		wb.setSecret(1234);
		wb.setTask(tb.build());
		addLeaderFieldToWorkMessage(wb);

		return wb.build();
	}


	public WorkMessage generateNewNodeReplicationMsg(MessageDetails details,String sender){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(MessageServer.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());

		Task.Builder tb = Task.newBuilder();
		tb.setChunkNo(details.getChunckId());
		tb.setNoOfChunks(details.getNoOfChuncks());
		tb.setChunk(ByteString.copyFrom(details.getByteData()));
		tb.setTaskType(TaskType.WRITE);
		tb.setSender(sender);
		tb.setFilename(details.getFileName());
		wb.setTask(tb);
		//TODO Generate secret
		wb.setSecret(1234);
		wb.setIsProcessed(false);
		wb.setWorktype(Worktype.LEADER_WRITE);
		addLeaderFieldToWorkMessage(wb);

		return wb.build();
	}

	/**
	 * Generates a message from the 1st node that the Monitor connects to. This method 
	 * creates a list of all the nodes in the network ending at the creator's own node ID.
	 * The idea is that when this message is passed around the nodes, each node will add
	 * his current status to the monitor message contained in the command message and will
	 * pass the message to the next node in the nodes list.
	 * @param message
	 * @return
	 */
	public CommandMessage initializeMonitorMsg(CommandMessage message, boolean isFirstNode, String requestID){

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(MessageServer.getNodeId());
		hb.setTime(System.currentTimeMillis());
		ClusterMonitor.Builder cmb = ClusterMonitor.newBuilder(message.getMonitorMsg());
		
		cmb.setProcessId(MessageServer.getNodeId(),MessageServer.getNodeId());
		//TODO Get the queue sizes.
		cmb.setEnqueued(MessageServer.getNodeId(), QueueManager.getInstance().getInboundCommQSize()+
				QueueManager.getInstance().getInboundWorkQSize());
		cmb.setProcessed(MessageServer.getNodeId(), NodeState.getInstance().getProcessed());
		cmb.setStolen(MessageServer.getNodeId(), NodeState.getInstance().getStolen());
		cmb.setTick(cmb.getTick()+1);

		//Initialize the circle of nodes in the network.
		CommandMessage.Builder cb = CommandMessage.newBuilder(message);
		cb.setMonitorMsg(cmb.build());
		cb.setHeader(hb.build());

		if(isFirstNode){
			//TODO Get the cluster ID from the routing conf.
			cmb.setClusterId(1);
			cmb.setNumNodes(NodeChannelManager.getNode2ChannelMap().size()+1);
			//Build the nextNodeIds field. Populate it with the IDs of all the nodes in the network.
			for(Integer nodeID : NodeChannelManager.getNode2ChannelMap().keySet()){
				cb.getNextNodeIdsList().add(nodeID);
			}
			//Add the current node ID to the list.
			cb.getNextNodeIdsList().add(MessageServer.getNodeId());
			//Set the request ID
			cb.setMessage(requestID);

		}
		return cb.build();

	}

	/**
	 * Generates a Heartbeat msg sent from leader to slave.
	 * @return
	 */
	public WorkMessage generateHeartbeat(){
		return null;
	}

	/**
	 * Generate Heartbeat response sent from the slave to the leader. Includes the system CPU utilization.
	 * @return
	 */
	public WorkMessage generateHeartbeatResponse(){
		return null;
	}


	private void addLeaderFieldToWorkMessage(WorkMessage.Builder wb) {
		if (NodeChannelManager.currentLeaderID == 0) {
			wb.setStateOfLeader(StateOfLeader.LEADERUNKNOWN);
		} else if (NodeChannelManager.currentLeaderID == conf.getNodeId()) {
			// Current Node is the leader
			wb.setStateOfLeader(StateOfLeader.LEADERALIVE);
		} else {
			wb.setStateOfLeader(StateOfLeader.LEADERKNOWN);
		}
	}

	public static void setRoutingConf(RoutingConf routingConf) {
		MessageGeneratorUtil.conf = routingConf;
	}

}
