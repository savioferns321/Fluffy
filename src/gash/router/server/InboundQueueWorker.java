package gash.router.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.QueueManager.CommandMessageChannelCombo;
import gash.router.server.edges.EdgeMonitor;

public class InboundQueueWorker extends Thread{

	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger("InboundQueueWorker");
	
	public InboundQueueWorker(QueueManager poller) {
		super();
		this.manager = poller;
		if (poller.inbound == null)
			throw new RuntimeException("Poller has a null queue");
	}

	@Override
	public void run() {
		
		//Poll the queue for messages
		while(true){
			try {
				CommandMessageChannelCombo currCombo = manager.dequeueInboundMsg();
				switch (currCombo.getCommandMessage().getTask().getTaskType()) {
				case WRITE:
					//TODO Write it to this(master) node
					Thread.sleep(10000);
					logger.info("Finished processing task "+currCombo.getCommandMessage().getTask().getFilename());
					System.out.println("Finished processing task "+currCombo.getCommandMessage().getTask().getFilename());
					System.out.flush();
					break;
					
				case READ:
					//TODO Forward it to the a slave node
					Thread.sleep(10000);
					logger.info("Finished processing task "+currCombo.getCommandMessage().getTask().getFilename());
					System.out.println("Finished processing task "+currCombo.getCommandMessage().getTask().getFilename());
					System.out.flush();
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
}
