package gash.router.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.QueueManager.CommandMessageChannelCombo;
import gash.router.server.QueueManager.WorkMessageChannelCombo;
import io.netty.channel.ChannelFuture;

public class OutboundWorker extends Thread{
	
	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(OutboundWorker.class);
	
	public OutboundWorker(QueueManager manager) {
		super();
		this.manager = manager;
		if (manager.inboundWorkQ == null)
			throw new RuntimeException("Manager has a null queue");
	}
	
	
	@Override
	public void run() {
		while (true) {

			try {
				// block until a message is enqueued
				WorkMessageChannelCombo msg = QueueManager.getInstance().dequeueOutboundWork();

				if (logger.isDebugEnabled())
					logger.debug("Outbound management message routing to node " + msg.getWorkMessage().getHeader().getDestination());

				if (msg.getChannel().isWritable()) {
					boolean rtn = false;
					if (msg.getChannel()!= null && msg.getChannel().isOpen() && msg.getChannel().isWritable()) {
						ChannelFuture cf = msg.getChannel().write(msg);

						cf.awaitUninterruptibly();
						rtn = cf.isSuccess();
						if (!rtn)
							manager.returnOutboundWork(msg);
					}

				} else {
					logger.info("channel to node " + msg.getWorkMessage().getHeader().getDestination() + " is not writable");
					manager.returnOutboundWork(msg);
				}
			} catch (InterruptedException ie) {
				break;
			} catch (Exception e) {
				logger.error("Unexpected management communcation failure", e);
				break;
			}
		}
	}
}