package gash.router.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.persistence.DataReplicationManager;
import gash.router.persistence.Dbhandler;
import gash.router.persistence.ReplicationInfo;
import gash.router.server.QueueManager.CommandMessageChannelCombo;
import io.netty.channel.Channel;
import pipe.common.Common.Header;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;
import routing.Pipe.Task;

public class InboundCommander extends Thread{

	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(InboundCommander.class);

	public InboundCommander(QueueManager poller) {
		super();
		this.manager = poller;
		if (poller.inboundCommQ == null)
			throw new RuntimeException("Poller has a null queue");
	}

	@Override
	public void run() {

		//Poll the queue for messages
		while(true){
			boolean isSuccess = false;
			try {
				CommandMessageChannelCombo currCombo = manager.dequeueInboundCommmand();
				Task task = currCombo.getCommandMessage().getTask();
				switch (currCombo.getCommandMessage().getTask().getTaskType()) {
				case WRITE:
					//TODO Write it to this(master) node. Send ACK to the client and asynchronously start replication on the remaining servers.

					/*Thread.sleep(10000);
					 logger.info("Finished processing task "+currCombo.getCommandMessage().getTask().getFilename());
					System.out.println("Finished processing task "+currCombo.getCommandMessage().getTask().getFilename());
					System.out.flush();					
					 */
					
					//Writing to itself
					try {
						Dbhandler.addFile(task.getFilename(), currCombo.getCommandMessage().getFileContent().toByteArray());
						isSuccess = true;
					} catch (Exception e) {
						e.printStackTrace();
						logger.error(e.getMessage());
					}

					//Starting replication
					//TODO is replication working properly?
					DataReplicationManager.getInstance().replicate(new ReplicationInfo(task.getFilename(), currCombo.getCommandMessage().getFileContent().toByteArray(), 0));

					
					//Send ACK to the client
					CommandMessage response = generateResponseMessage(isSuccess);
					manager.enqueueOutboundCommand(response, currCombo.getChannel());
					logger.info("Finished processing task "+currCombo.getCommandMessage().getTask().getFilename()+" from client : "+currCombo.getChannel().remoteAddress());
					
					break;

				case READ:
					//TODO Forward it to the a slave node
					/*
					 * This is a command message, so this is directly from a client and this node is a leader.
					 * So find a node who can process this request.
					 * Then generate a work message to get the file from that node.
					 * Also, store the client channel so that we can send the read file to him directly.
					 */
					
					//Get the next node to delegate the request
					Channel nextChannel= NodeChannelManager.getNextReadChannel();
					
					//TODO Generate proper work message to send to next client. Current implementation is faulty.
					WorkMessage message = generateDelegationMessage(currCombo.getCommandMessage());
					
					//TODO Should we directly write these messages on the channel?	
					nextChannel.writeAndFlush(message);
										
					//Thread.sleep(10000);
					logger.info("Finished processing task "+currCombo.getCommandMessage().getTask().getFilename());
					NodeChannelManager.addClientToMap(currCombo.getCommandMessage(), nextChannel);
					break;	

				default:
					break;
				}

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				logger.error(e.getMessage());				
				e.printStackTrace();
			}
		}
	}

	//TODO Place this method in a relevant class
	public CommandMessage generateResponseMessage(boolean isSuccess){
		Header.Builder hb = Header.newBuilder();
		//TODO Get node ID
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());


		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		if(isSuccess)
			rb.setMessage(" File saved ");
		else
			rb.setMessage(" Operation Failed ");

		return rb.build();

	}
	
	public WorkMessage generateDelegationMessage(CommandMessage commandMessage){
		
		Header.Builder hb = Header.newBuilder();
		//TODO Get node ID
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		
		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		//TODO Set the filename in the request
		commandMessage.getTask().getFilename();
		
		//TODO Set the secret
		wb.setSecret(1234);
		
		return wb.build();
	}
}
