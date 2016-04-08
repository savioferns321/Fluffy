package gash.router.raft.leaderelection;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf;
import gash.router.server.NodeChannelManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import pipe.election.Election.RaftMessage.ElectionAction;
import pipe.work.Work.WorkMessage;

public class ElectionManagement {

	protected static AtomicReference<ElectionManagement> instance = new AtomicReference<ElectionManagement>();

	private static RoutingConf routingConf = null;
	private static RaftStateMachine raftStateMachine;
	private static Timer electionTimer;
	private static ConcurrentHashMap<Integer, Boolean> voteCheckMap = new ConcurrentHashMap<Integer, Boolean>();
	private static final int MINIMUM_NUMBER_OF_NODES_REQUIRED = 1;
	private static int currentVoteCount = 1;

	private static Logger logger;

	public ElectionManagement() {
		logger = LoggerFactory.getLogger(getClass());
	}

	public static ElectionManagement getInstance() {
		if (instance.get() == null) {
			instance.compareAndSet(null, new ElectionManagement());
		}
		return instance.get();
	}

	public static ElectionManagement initElectionManagement(RoutingConf routingConf) {
		ElectionManagement.routingConf = routingConf;
		ElectionManagement.raftStateMachine = new RaftStateMachine();
		electionTimer = new Timer();

		MessageBuilder.setRoutingConf(routingConf);
		instance.compareAndSet(null, new ElectionManagement());
		return instance.get();
	}

	public static boolean isReadyForElection() {
		return NodeChannelManager.numberOfActiveChannels() >= MINIMUM_NUMBER_OF_NODES_REQUIRED ? true : false;
	}

	public static void startElection() {
		startElectionTimer();
	}

	private static void sendMessage() throws Exception {
		if (raftStateMachine.isCandidate()) {
			System.out.println("------ Building the election Message -----");
			WorkMessage electionMessage = MessageBuilder.buildElectionMessage();
			System.out.println("Election Message " + electionMessage.toString());
			NodeChannelManager.broadcast(electionMessage);
		}
	}

	// To process incoming election work messages from other nodes
	public static synchronized void processElectionMessage(Channel incomingChannel, WorkMessage electionWorkMessage) {
		// If this node receives any Request Vote message
		if (electionWorkMessage.getRaftMessage().getAction() == ElectionAction.REQUESTVOTE) {
			if (raftStateMachine.isFollower()) {
				WorkMessage electionResponseMessage = MessageBuilder.buildElectionResponseMessage(
						electionWorkMessage.getRaftMessage().getRequestVote().getCandidateId(), true);
				if (!voteCheckMap.containsKey(routingConf.getNodeId())) {
					voteCheckMap.put(routingConf.getNodeId(), true);
				}
				System.out.println("Vote Casted in favour of : "
						+ electionResponseMessage.getRaftMessage().getRequestVote().getCandidateId());
				// incomingChannel.writeAndFlush(electionResponseMessage);
				
				ChannelFuture cf = incomingChannel.write(electionResponseMessage);
				incomingChannel.flush();
				cf.awaitUninterruptibly();
				if (cf.isDone() && !cf.isSuccess()) {
					logger.error("Failed to send replication message to server");
				}
			}
		}

		// If the other node is casting vote in the favor of this node
		if (electionWorkMessage.getRaftMessage().getAction() == ElectionAction.VOTE && raftStateMachine.isCandidate()) {
			// Count the votes, If there is majority, send out leader message to
			// all.
			// To check whether the this request vote is for current node or not
			if (electionWorkMessage.getRaftMessage().getRequestVote().getCandidateId() == routingConf.getNodeId()) {
				boolean amILeader = decideIfLeader(electionWorkMessage);
				if (amILeader) {
					try {
						raftStateMachine.becomeLeader();
						System.out.println("Node " + routingConf.getNodeId() + " became leader");
						WorkMessage leaderResponseMessage = MessageBuilder
								.buildLeaderResponseMessage(routingConf.getNodeId());
						NodeChannelManager.broadcast(leaderResponseMessage);
						// Current Node is the leader
						NodeChannelManager.currentLeaderID = routingConf.getNodeId();
						NodeChannelManager.currentLeaderAddress = leaderResponseMessage.getRaftMessage()
								.getLeaderHost();
						
						//Start the scheduler to send node status to the monitor.
						
					} catch (Exception exception) {
						System.out.println("An error has occured while broadcasting the message.");
					}
				}
			}
		}

		if (electionWorkMessage.getRaftMessage().getAction() == ElectionAction.LEADER) {
			try {

				System.out.println("Current Leader ID : " + electionWorkMessage.getRaftMessage().getLeaderId());
				// Saving current leader id in NodeChannelManager to use it
				// across the node
				NodeChannelManager.currentLeaderID = electionWorkMessage.getRaftMessage().getLeaderId();
				NodeChannelManager.currentLeaderAddress = electionWorkMessage.getRaftMessage().getLeaderHost();

			} catch (Exception exception) {
				System.out.println("Something is not correct");
			}
		}
	}

	private static boolean decideIfLeader(WorkMessage electionWorkMessage) {
		// TODO put logic to count the number of votes received so far and
		// decide the leader
		if (electionWorkMessage.getRaftMessage().getRequestVote().getVoteGranted()) {
			/*
			 * currentVoteCount++; if (currentVoteCount >
			 * ((NodeChannelManager.getNode2ChannelMap().size() / 2) + 1))
			 * return true; else return false;
			 */
			return true;
		}
		return false;
	}

	public static void resetElection() {
		// Reset the State of the nodes and cluster
		raftStateMachine.becomeFollower();
		NodeChannelManager.currentLeaderID = 0;
		voteCheckMap = new ConcurrentHashMap<Integer, Boolean>();
	}

	private static void startElectionTimer() {
		// Election timer
		int currentTimeout = RandomTimeoutGenerator.randTimeout() * routingConf.getNodeId();
		System.out.println("The Election Timeout for this node is : " + currentTimeout);
		electionTimer.schedule(new ElectionTimer(), (long) currentTimeout, (long) currentTimeout);
	}

	private static class ElectionTimer extends TimerTask {
		@Override
		public void run() {
			System.out.println("Timer Expired for " + routingConf.getNodeId());

			// Cancel the ongoing scheduled timer call electionTimer.cancel();
			if (voteCheckMap.containsKey(routingConf.getNodeId()) && voteCheckMap.get(routingConf.getNodeId())) {
				electionTimer.cancel();
				voteCheckMap.put(routingConf.getNodeId(), false);
			}
			// check if we haven't received the leader message from anybody else
			// if not set it as candidate
			if (raftStateMachine.isFollower() && !voteCheckMap.containsKey(routingConf.getNodeId())) {
				raftStateMachine.becomeCandidate();
				try {
					ElectionManagement.sendMessage();
				} catch (Exception exception) {
					System.out.println("An error has occured while sending message");
				}
			}

			if (raftStateMachine.isLeader()) {
				electionTimer.cancel();
			}
		}

	}

}