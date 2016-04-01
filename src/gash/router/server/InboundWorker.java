package gash.router.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.persistence.Dbhandler;
import gash.router.persistence.MessageDetails;
import gash.router.server.QueueManager.CommandMessageChannelCombo;
import gash.router.server.QueueManager.WorkMessageChannelCombo;
import gash.server.util.MessageGeneratorUtil;
import io.netty.channel.Channel;
import pipe.common.Common.Task;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class InboundWorker extends Thread {

	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(InboundWorker.class);

	public InboundWorker(QueueManager poller) {
		super();
		this.manager = poller;
		if (poller.inboundWorkQ == null)
			throw new RuntimeException("Poller has a null queue");
	}

	//TODO Implement method for polling the queue
	@Override
	public void run() {
		try {
			//Poll the queue for messages
			WorkMessageChannelCombo wmCombo = manager.dequeueInboundWork();
			WorkMessage currWork = wmCombo.getWorkMessage();
			Channel currChannel = wmCombo.getChannel();
			Task t = currWork.getTask();
			while(true){
				//Message can be from leader or from a slave.
				switch (currWork.getWorktype()) {
				case LEADER_READ:
					//Message from leader to READ a file stored on this machine
					logger.info(" Received msg to read a file on this system ");
					int chunkCount = Dbhandler.getChuncks(t.getFilename());
					for (int i = 1; i <= chunkCount; i++) {
						//Get the file chunks
						MessageDetails details = Dbhandler.getFilewithChunckId(t.getFilename(), i);
						//Construct a work message for each chunk. Set the destination as the leader. Set the message type as SLAVE_READ_DONE
						WorkMessage msg = MessageGeneratorUtil.getInstance().generateDelegationRespMsg(t, details.getByteData(), i, chunkCount, currWork.getRequestId());
						//Send these messages to the outbound work queue
						manager.enqueueOutboundWork(msg, currChannel);
					}
					
					break;

				case SLAVE_READ_DONE:
					//Message from a slave which has sent some data to be sent to the client.
					logger.info(" Slave returned some data to be forwarded to client ");				
					//Get the client channel from the map
					CommandMessageChannelCombo cmCombo = NodeChannelManager.getClientChannelFromMap(currWork.getRequestId());
					Channel cliChannel =  cmCombo.getChannel();
					
					//Decrement the client's chunk count
					cmCombo.decrementChunkCount();
					
					//Handle the client channel map, remove the client's channel if it is the last chunk
					if(cmCombo.getChunkCount() ==0){
						NodeChannelManager.removeClientChannelFromMap(currWork.getRequestId());
					}
					
					//Create a command message for this chunk
					CommandMessage outputMsg = MessageGeneratorUtil.getInstance().forwardChunkToClient(currWork);
					//Send the generated command message to the outbound command queue.
					manager.enqueueOutboundCommand(outputMsg, cliChannel);

					break;

				case LEADER_WRITE:
					//Message from a leader to replicate/write some data.
					logger.info("Got message to replicate some data ");
					//Write the data to in memory db / persistent DB depending on chunk size
					Dbhandler.addFile(t.getFilename(), t.getChunk().toByteArray(), t.getNoOfChunks(), t.getChunkNo());
					//Generate a work message with flag for Worktype SLAVE_WRITTEN.
					WorkMessage wm = MessageGeneratorUtil.getInstance().generateReplicationAckMessage(currWork);
					//Send this message to the outbound queue
					manager.enqueueOutboundWork(wm, currChannel);

					break;

				case SLAVE_WRITTEN:
					//TODO Message from a slave saying that it has completed the replication.
					//To be implemented later after completing basic functionality.
					logger.info("Slave replicated the data ");

					break;

				default:
					break;
				}
			}
		} catch (InterruptedException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		
	}
	
}
