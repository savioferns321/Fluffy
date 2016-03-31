package gash.router.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.QueueManager.WorkMessageChannelCombo;

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
			WorkMessageChannelCombo message = manager.dequeueInboundWork();
			//Poll the queue for messages
			while(true){
				
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
