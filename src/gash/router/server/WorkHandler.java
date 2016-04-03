/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.server;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.persistence.DataReplicationManager;
import gash.router.raft.leaderelection.ElectionManagement;
import gash.router.raft.leaderelection.MessageBuilder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common.Failure;
import pipe.election.Election.LeaderStatus.LeaderQuery;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.StateOfLeader;
import pipe.work.Work.WorkState;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class WorkHandler extends SimpleChannelInboundHandler<WorkMessage> {
	protected static Logger logger = LoggerFactory.getLogger("work");
	protected ServerState state;
	protected boolean debug = false;
	// private static Timer electionTimer;
	private static ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();

	public WorkHandler(ServerState state) {
		// electionTimer = new Timer();

		if (state != null) {
			this.state = state;
		}
	}

	/**
	 * override this method to provide processing behavior. T
	 * 
	 * @param msg
	 */
	public void handleMessage(WorkMessage msg, Channel channel) {
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		/*if (debug)
			PrintUtil.printWork(msg);
*/
		// TODO How can you implement this without if-else statements?
		try {
			if (msg.getStateOfLeader() == StateOfLeader.LEADERALIVE) {
				//exec.shutdownNow();
				//int currentTimeout = RandomTimeoutGenerator.randTimeout() * this.state.getConf().getNodeId();
				//exec = Executors.newSingleThreadScheduledExecutor();
				//exec.schedule(new ElectionTImer(), (long) currentTimeout, TimeUnit.MILLISECONDS);
				// System.out.println("Leader is Alive ");
				// Thread.sleep(1000);
				// electionTimer.purge();
				// this.startElectionTimer();
			} else if (msg.hasLeader() && msg.getLeader().getAction() == LeaderQuery.WHOISTHELEADER) {
				WorkMessage buildNewNodeLeaderStatusResponseMessage = MessageBuilder
						.buildNewNodeLeaderStatusResponseMessage(NodeChannelManager.currentLeaderID,
								NodeChannelManager.currentLeaderAddress);
				channel.writeAndFlush(buildNewNodeLeaderStatusResponseMessage);
				
				//Sent the newly discovered node all the data on this node.
				DataReplicationManager.getInstance().replicateToNewNode(channel);
				
				
			} else if (msg.hasLeader() && msg.getLeader().getAction() == LeaderQuery.THELEADERIS) {
				NodeChannelManager.currentLeaderID = msg.getLeader().getLeaderId();
				NodeChannelManager.currentLeaderAddress = msg.getLeader().getLeaderHost();
				logger.debug("The leader is " + NodeChannelManager.currentLeaderID);
			}

			if (msg.hasBeat()) {
				Heartbeat hb = msg.getBeat();
				// logger.info("heartbeat from " + msg.getHeader().getNodeId());

				// TODO Generate Heartbeat response. Currently returns null.
				// Write
				// this response to the channel synchronously.
				// WorkMessage message =
				// MessageGeneratorUtil.getInstance().generateHeartbeat();
				WorkMessage message = msg;
				synchronized (channel) {
					channel.writeAndFlush(message);
				}

			} else if (msg.hasTask()) {

				// Enqueue it to the inbound work queue
				logger.info("Received inbound work ");
				QueueManager.getInstance().enqueueInboundWork(msg, channel);

			} else if (msg.hasFlagRouting()) {
				logger.info("Routing information recieved " + msg.getHeader().getNodeId());
				logger.info("Routing Entries: " + msg.getRoutingEntries());

				System.out.println("CONNECTED TO NEW NODE---------------------------------");
				SocketAddress remoteAddress = channel.remoteAddress();
				InetSocketAddress addr = (InetSocketAddress) remoteAddress;

				state.getEmon().createOutboundIfNew(msg.getHeader().getNodeId(), addr.getHostName(), 9999);

				System.out.println(addr.getHostName());

			} else if (msg.hasNewNode()) {
				logger.info("NEW NODE TRYING TO CONNECT " + msg.getHeader().getNodeId());
				WorkMessage wm = state.getEmon().createRoutingMsg();
				channel.writeAndFlush(wm);
				//TODO New node has been detected. Push all your data to it now.
				SocketAddress remoteAddress = channel.remoteAddress();
				InetSocketAddress addr = (InetSocketAddress) remoteAddress;

				state.getEmon().createInboundIfNew(msg.getHeader().getNodeId(), addr.getHostName(), 5100);
				state.getEmon().getInboundEdges().getNode(msg.getHeader().getNodeId()).setChannel(channel);
				
			} else if (msg.hasPing()) {
				logger.info("ping from " + msg.getHeader().getNodeId());
				boolean p = msg.getPing();
				WorkMessage.Builder rb = WorkMessage.newBuilder();
				rb.setPing(true);
				// channel.writeAndFlush(rb.build());
			} else if (msg.getHeader().getElection()) {
				// call the election handler to handle this request
				System.out.println(" ---- Message for election has come ---- ");
				ElectionManagement.processElectionMessage(channel, msg);
			} else if (msg.hasErr()) {
				Failure err = msg.getErr();
				logger.error("failure from " + msg.getHeader().getNodeId());
				// PrintUtil.printFailure(err);
			} else if (msg.hasState()) {
				WorkState s = msg.getState();
			} else {
				logger.info("Executing from work handler ");

			}
		} catch (NullPointerException e) {
			System.out.println("Null pointer has occured");
		}

		catch (Exception e) {
			// TODO add logging
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(state.getConf().getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			WorkMessage.Builder rb = WorkMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.writeAndFlush(rb.build());
		}

		System.out.flush();

	}

	private void startElectionTimer() {
		// Election timer
		/*
		 * int currentTimeout = RandomTimeoutGenerator.randTimeout() *
		 * this.state.getConf().getNodeId(); electionTimer = new Timer();
		 * electionTimer.schedule(new ElectionTImer(), (long) currentTimeout,
		 * (long) currentTimeout);
		 */
	}

	/**
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 * 
	 * @param ctx
	 *            The channel the message was received from
	 * @param msg
	 *            The message
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, WorkMessage msg) throws Exception {
		Channel channel = ctx.channel();
		handleMessage(msg, channel);

		Thread.sleep(300);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}