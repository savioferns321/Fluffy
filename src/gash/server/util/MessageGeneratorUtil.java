package gash.server.util;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import gash.router.container.RoutingConf;
import gash.router.server.MessageServer;
import gash.router.server.NodeChannelManager;
import pipe.common.Common.Header;
import pipe.common.Common.Task;
import pipe.common.Common.Task.TaskType;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.StateOfLeader;
import pipe.work.Work.WorkMessage.Worktype;
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
		tb.setChunk(message.getFileContent());
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
