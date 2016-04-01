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
 * Handles the input queue for the server node. Polls the queue and runs tasks
 * from it whenever there is an element in the queue.
 * 
 * @author savio.fernandes
 * 
 */
public class QueueManager {

	protected static Logger logger = LoggerFactory.getLogger(QueueManager.class);
	protected static AtomicReference<QueueManager> instance = new AtomicReference<QueueManager>();

	protected LinkedBlockingDeque<CommandMessageChannelCombo> inboundCommQ;
	protected LinkedBlockingDeque<CommandMessageChannelCombo> outboundCommQ;
	protected InboundCommander inboundCommmander;
	protected OutboundCommander outboundCommmander;

	protected LinkedBlockingDeque<WorkMessageChannelCombo> inboundWorkQ;
	protected LinkedBlockingDeque<WorkMessageChannelCombo> outboundWorkQ;
	protected InboundCommander inboundWorker;
	protected OutboundCommander outboundWorker;

	public static QueueManager initManager() {
		instance.compareAndSet(null, new QueueManager());
		return instance.get();
	}

	public static QueueManager getInstance() {
		if (instance == null)
			instance.compareAndSet(null, new QueueManager());
		return instance.get();
	}

	public QueueManager() {
		logger.info(" Started the Manager ");

		inboundCommQ = new LinkedBlockingDeque<QueueManager.CommandMessageChannelCombo>();
		outboundCommQ = new LinkedBlockingDeque<QueueManager.CommandMessageChannelCombo>();
		inboundCommmander = new InboundCommander(this);
		inboundCommmander.start();
		outboundCommmander = new OutboundCommander(this);
		outboundCommmander.start();

		inboundWorkQ = new LinkedBlockingDeque<QueueManager.WorkMessageChannelCombo>();
		outboundWorkQ = new LinkedBlockingDeque<QueueManager.WorkMessageChannelCombo>();
		inboundWorker = new InboundCommander(this);
		inboundWorker.start();
		outboundWorker = new OutboundCommander(this);
		outboundWorker.start();
	}

	/*
	 * Functions for Command Messages
	 */
	public void enqueueInboundCommmand(CommandMessage message, Channel ch) {
		try {
			CommandMessageChannelCombo entry = new CommandMessageChannelCombo(ch, message);
			inboundCommQ.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	public CommandMessageChannelCombo dequeueInboundCommmand() throws InterruptedException {
		return inboundCommQ.take();
	}

	public void enqueueOutboundCommand(CommandMessage message, Channel ch) {
		try {
			CommandMessageChannelCombo entry = new CommandMessageChannelCombo(ch, message);
			outboundCommQ.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	public CommandMessageChannelCombo dequeueOutboundCommmand() throws InterruptedException {
		return outboundCommQ.take();
	}

	public void returnOutboundCommand(CommandMessageChannelCombo msg) throws InterruptedException {
		outboundCommQ.putFirst(msg);
	}

	public void returnInboundCommand(CommandMessageChannelCombo msg) throws InterruptedException {
		inboundCommQ.putFirst(msg);
	}

	/*
	 * End of Command Message methods
	 */

	/*
	 * Work Message methods
	 */

	public void enqueueInboundWork(WorkMessage message, Channel ch) {
		try {
			WorkMessageChannelCombo entry = new WorkMessageChannelCombo(ch, message);
			inboundWorkQ.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	public WorkMessageChannelCombo dequeueInboundWork() throws InterruptedException {
		return inboundWorkQ.take();
	}

	public void enqueueOutboundWork(WorkMessage message, Channel ch) {
		try {
			WorkMessageChannelCombo entry = new WorkMessageChannelCombo(ch, message);
			outboundWorkQ.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	public WorkMessageChannelCombo dequeueOutboundWork() throws InterruptedException {
		return outboundWorkQ.take();
	}

	public void returnOutboundWork(WorkMessageChannelCombo msg) throws InterruptedException {
		outboundWorkQ.putFirst(msg);
	}

	public void returnInboundWork(WorkMessageChannelCombo msg) throws InterruptedException {
		inboundWorkQ.putFirst(msg);
	}

	/*
	 * End of Work Message methods
	 */

	/**
	 * Object which is stored in the Master's queue. Channel is stored so we can
	 * directly send a response to the client on this channel. It only handles
	 * Command messages.
	 * 
	 * @author savio
	 *
	 */
	public class CommandMessageChannelCombo {
		private Channel channel;
		private CommandMessage commandMessage;
		private int chunkCount = 0;

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

		public void setChunkCount(int chunkCount) {
			this.chunkCount = chunkCount;
		}

		public int getChunkCount() {
			return chunkCount;
		}

		public synchronized void decrementChunkCount() {
			chunkCount--;
		}

	}

	/**
	 * Object which is stored in the Master's queue. Channel is stored so we can
	 * directly send a response to the client on this channel. It only handles
	 * Work messages.
	 * 
	 * @author savio
	 *
	 */
	public class WorkMessageChannelCombo {
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

		public WorkMessage getWorkMessage() {
			return workMessage;
		}

	}

}
