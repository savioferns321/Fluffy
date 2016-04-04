package gash.router.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.raft.leaderelection.NodeState;
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
				WorkMessageChannelCombo msg = manager.dequeueOutboundWork();
				NodeState.getInstance().incrementProcessed();
				if (logger.isDebugEnabled())
					logger.debug("Outbound management message routing to node " + msg.getWorkMessage().getHeader().getDestination());
				
				if (msg.getChannel()!= null && msg.getChannel().isOpen()) {
					boolean rtn = false;
					if (msg.getChannel().isWritable()) {
						ChannelFuture cf = msg.getChannel().writeAndFlush(msg.getWorkMessage());
						cf.awaitUninterruptibly();
						rtn = cf.isSuccess();
						logger.info("Wrote msg to the channel ? "+rtn);
						if (!rtn)
							manager.returnOutboundWork(msg);
					}

				} else {
					logger.info("channel to node " + msg.getWorkMessage().getHeader().getDestination() + " is not writable");
					logger.info("Is channel null : "+(msg.getChannel() == null));
					//manager.returnOutboundWork(msg);
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
