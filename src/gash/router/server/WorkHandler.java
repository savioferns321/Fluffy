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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.server.util.MessageGeneratorUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common.Failure;
import pipe.common.Common.Task;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;
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
	protected boolean debug = true;

	public WorkHandler(ServerState state) {
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

		if (debug)
			//PrintUtil.printWork(msg);

		// TODO How can you implement this without if-else statements?
		try {
			if (msg.hasBeat()) {
				Heartbeat hb = msg.getBeat();
				//logger.info("heartbeat from " + msg.getHeader().getNodeId());

				//TODO Generate Heartbeat response. Currently returns null. Write this response to the channel synchronously.
				//WorkMessage message = MessageGeneratorUtil.getInstance().generateHeartbeat();
				WorkMessage message = msg;
				synchronized (channel) {
					channel.writeAndFlush(message);
				}

			} else if (msg.hasFlagRouting()) {
				logger.info("Routing information recieved " + msg.getHeader().getNodeId());
				logger.info("Routing Entries: " + msg.getRoutingEntries());

			} else if (msg.hasNewNode()) {
				logger.info("NEW NODE TRYING TO CONNECT " + msg.getHeader().getNodeId());
				WorkMessage wm = state.getEmon().createRoutingMsg();
				channel.writeAndFlush(wm);

			} else if (msg.hasPing()) {
				logger.info("ping from " + msg.getHeader().getNodeId());
				boolean p = msg.getPing();
				WorkMessage.Builder rb = WorkMessage.newBuilder();
				rb.setPing(true);
				//channel.writeAndFlush(rb.build());
			} else if (msg.hasErr()) {
				Failure err = msg.getErr();
				logger.error("failure from " + msg.getHeader().getNodeId());
				// PrintUtil.printFailure(err);
			} else if (msg.hasTask()) {

				//Enqueue it to the inbound work queue
				QueueManager.getInstance().enqueueInboundWork(msg, channel);

			} else if (msg.hasState()) {
				WorkState s = msg.getState();
			}else{
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
			channel.write(rb.build());
		}

		System.out.flush();

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

		// Should happen via queue?

		/*
		 * WorkState.Builder sb = WorkState.newBuilder(); sb.setEnqueued(-1);
		 * sb.setProcessed(-1);
		 * 
		 * WorkMessage.Builder wb = WorkMessage.newBuilder();
		 * wb.setSecret(1234);
		 * 
		 * Header.Builder hb = Header.newBuilder();
		 * hb.setNodeId(state.getConf().getNodeId());
		 * hb.setDestination(msg.getHeader().getNodeId());
		 * hb.setTime(System.currentTimeMillis());
		 * 
		 * Heartbeat.Builder heartbeat = Heartbeat.newBuilder();
		 * heartbeat.setState(sb);
		 * 
		 * wb.setBeat(heartbeat); wb.setHeader(hb);
		 * 
		 * channel.writeAndFlush(wb.build());
		 */
		/*Thread.sleep(1000);
		ctx.channel().writeAndFlush(msg);*/
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}