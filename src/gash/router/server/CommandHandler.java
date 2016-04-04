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

import gash.router.container.RoutingConf;
import gash.router.server.QueueManager.CommandMessageChannelCombo;
import gash.server.util.MessageGeneratorUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common.Failure;
import pipe.monitor.Monitor.ClusterMonitor;
import routing.Pipe.CommandMessage;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class CommandHandler extends SimpleChannelInboundHandler<CommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger(CommandHandler.class);
	protected RoutingConf conf;

	public CommandHandler(RoutingConf conf) {

		if (conf != null) {
			this.conf = conf;
		}
	}

	/**
	 * override this method to provide processing behavior. This implementation
	 * mimics the routing we see in annotating classes to support a RESTful-like
	 * behavior (e.g., jax-rs).
	 * 
	 * @param msg
	 */
	public void handleMessage(CommandMessage msg, Channel channel) {
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		PrintUtil.printCommand(msg);

		try {
			// TODO How can you implement this without if-else statements?
			if(msg.hasMonitorMsg()){
				//This is sent from the monitor. The monitor message needs to be populated.
				//TODO Determine is this is sent from Client-Server, Server-Server, or Server-Client
				ClusterMonitor clusterMsg = msg.getMonitorMsg();
				if(clusterMsg.getClusterId() == -1){

					/*case 1 : Client-Server(cluster-Id = -1).Populate the nextNodes list and its own load in the cluster
					msg. Send it to the next node in the list.*/
					//Store the client channel to be retrieved later.
					String requestID = NodeChannelManager.addClientToMap
							(QueueManager.getInstance().new CommandMessageChannelCombo(channel, msg));
					CommandMessage cmdMsg = MessageGeneratorUtil.getInstance().initializeMonitorMsg(msg,true, requestID);

					Channel nextNodeChannel = NodeChannelManager.getNode2ChannelMap()
							.get(cmdMsg.getNextNodeIdsList().remove(0));
					ChannelFuture cf = null;
					
					if(nextNodeChannel == null){
						//No node in the network to read from.
						logger.info("No other nodes in the network to retrieve for monitoring ");
						while(!channel.isWritable()){
							//Looping until channel is writable
						}
						cf = channel.writeAndFlush(cmdMsg);
					}else{
						logger.info("Found nodes in the network to retrieve for monitoring ");
						while(!nextNodeChannel.isWritable()){
							//Looping until channel is writable
						}
						cf = nextNodeChannel.writeAndFlush(cmdMsg);
					}
					if (cf.isDone() && !cf.isSuccess()) {
						logger.info("Failed to write the monitor message to the channel ");
					}
				}else{
					switch (msg.getNextNodeIdsCount()) {
					case 0:
						//case 3 : Message returned back to server. Send this message back to client.
						//Read the request ID from the message(set in the oneof message field) and get the client channel.
						Channel clientChannel = NodeChannelManager.getClientChannelFromMap(msg.getMessage()).getChannel();
						while(!clientChannel.isWritable()){
							//Looping until channel is writable
						}
						ChannelFuture cf = clientChannel.writeAndFlush(clientChannel);
						if (cf.isDone() && !cf.isSuccess()) {
							logger.info("Failed to write the monitor message to the channel ");
						}
						break;

					default:
						//case 2 : Message from another server. Populate with its own load. Needs to be forwarded to the next server.
						CommandMessage cmdMsg = MessageGeneratorUtil.getInstance().initializeMonitorMsg(msg,false, null);
						Channel nextNodeChannel = NodeChannelManager.getNode2ChannelMap()
								.get(cmdMsg.getNextNodeIdsList().remove(0));
						while(!nextNodeChannel.isWritable()){
							//Looping until channel is writable
						}
						cf = nextNodeChannel.writeAndFlush(cmdMsg);
						if (cf.isDone() && !cf.isSuccess()) {
							logger.info("Failed to write the monitor message to the channel ");
						}
						break;
					}
				}
			}


			if(msg.hasTask()){

				/**
				 * TODO Enqueue the command message and the channel into the server queue
				 */
				logger.info("Received task from " + msg.getHeader().getNodeId());
				System.out.println("Queuing task");
				System.out.flush();
				QueueManager.getInstance().enqueueInboundCommmand(msg, channel);
			}else			
				if (msg.hasPing()) {
					logger.info("ping from " + msg.getHeader().getNodeId());
				} else if (msg.hasMessage()) {
					logger.info(msg.getMessage());
				}  

		} catch (Exception e) {
			// TODO add logging
			try {

				Failure.Builder eb = Failure.newBuilder();
				eb.setId(conf.getNodeId());
				eb.setRefId(msg.getHeader().getNodeId());
				eb.setMessage(e.getMessage());
				CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
				rb.setErr(eb);
				while(!channel.isWritable()){
					//Looping until channel is writable
				}
				ChannelFuture cf = channel.writeAndFlush(rb.build());
				if (cf.isDone() && !cf.isSuccess()) {
					logger.info("Failed to write the message to the channel ");
				}
			}catch (Exception e2) {
				e.printStackTrace();
			}
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
	protected void channelRead0(ChannelHandlerContext ctx, CommandMessage msg) throws Exception {
		handleMessage(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}