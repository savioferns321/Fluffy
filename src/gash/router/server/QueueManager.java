package gash.router.server;

import java.rmi.UnexpectedException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

/**
 * Handles the input queue for the server node.
 * Polls the queue and runs tasks from it whenever there is an element in the queue.
 * @author savio.fernandes
 * 
 */
public class QueueManager{

	protected static Logger logger = LoggerFactory.getLogger("Manager");
	protected static AtomicReference<QueueManager> instance = new AtomicReference<QueueManager>();

	protected LinkedBlockingDeque<CommandMessageChannelCombo> inbound;
	protected LinkedBlockingDeque<CommandMessageChannelCombo> outbound;
	protected InboundQueueWorker inboundWorker;
	protected OutboundQueueWorker outboundWorker;
	
	public static QueueManager initManager() {
		instance.compareAndSet(null, new QueueManager());
		return instance.get();
	}
	
	public QueueManager() {
		logger.info(" Started the Manager ");
		inbound = new LinkedBlockingDeque<QueueManager.CommandMessageChannelCombo>();
		outbound = new LinkedBlockingDeque<QueueManager.CommandMessageChannelCombo>();
		inboundWorker = new InboundQueueWorker(this);
		inboundWorker.start();
		outboundWorker = new OutboundQueueWorker(this);
		outboundWorker.start();
	}
	
	public void enqueueInboundMsg(CommandMessage message, Channel ch) {
		try {
			CommandMessageChannelCombo entry = new CommandMessageChannelCombo(ch, message);
			inbound.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}
	
	public CommandMessageChannelCombo dequeueInboundMsg() throws InterruptedException{
		return inbound.take();
	}
	
	public void enqueueOutboundMsg(CommandMessage message, Channel ch) {
		try {
			CommandMessageChannelCombo entry = new CommandMessageChannelCombo(ch, message);
			outbound.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}
	
	public CommandMessageChannelCombo dequeueOutboundMsg() throws InterruptedException{
		return outbound.take();
	}
	
	
	public static QueueManager getInstance() throws UnexpectedException{
		if(instance!= null && instance.get()!= null){
			return instance.get();
		}
		throw new UnexpectedException(" Manager not started ");
	}
	
	public void returnOutboundMsg(CommandMessageChannelCombo msg) throws InterruptedException{
		outbound.putFirst(msg);
	}
	
	public void returnInboundMsg(CommandMessageChannelCombo msg) throws InterruptedException{
		inbound.putFirst(msg);
	}
	
	/**
	 * Object which is stored in the Master's queue. Channel is stored so we can directly send a response to the client on this channel.
	 * It only handles Command messages.
	 * @author savio
	 *
	 */
	public class CommandMessageChannelCombo{
		private Channel channel;
		private CommandMessage commandMessage;

		public CommandMessageChannelCombo(Channel channel, CommandMessage commandMessage) {
			super();
			this.channel = channel;
			this.commandMessage = commandMessage;
		}

		public Channel getChannel() {
			return channel;
		}
		public CommandMessage getCommandMessage() {
			return commandMessage;
		}

	}
	
	/**
	 * Object which is stored in the Master's queue. Channel is stored so we can directly send a response to the client on this channel.
	 * It only handles Work messages.
	 * @author savio
	 *
	 */
	public class WorkMessageChannelCombo{
		private Channel channel;
		private WorkMessage workMessage;

		public WorkMessageChannelCombo(Channel channel, WorkMessage workMessage) {
			super();
			this.channel = channel;
			this.workMessage = workMessage;
		}

		public Channel getChannel() {
			return channel;
		}
		public WorkMessage getCommandMessage() {
			return workMessage;
		}

	}


}
