package gash.router.raft.leaderelection;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import gash.router.container.RoutingConf;
import gash.router.server.NodeChannelManager;
import io.netty.channel.Channel;
import pipe.election.Election.RaftMessage.ElectionAction;
import pipe.work.Work.WorkMessage;

public class ElectionManagement {

	protected static AtomicReference<ElectionManagement> instance = new AtomicReference<ElectionManagement>();

	private static RoutingConf routingConf = null;
	private static RaftStateMachine raftStateMachine;
	private static Timer electionTimer;
	private static ConcurrentHashMap<Integer, Boolean> voteCheckMap = new ConcurrentHashMap<Integer, Boolean>();
	private static final int MINIMUM_NUMBER_OF_NODES_REQUIRED = 2;
	private static int currentVoteCount = 1;
	private NodeState currentNodeState;

	public ElectionManagement() {

	}

	public ElectionManagement(NodeState nodeState) {
		this.currentNodeState = nodeState;
	}

	public static ElectionManagement getInstance() {
		if (instance.get() == null) {
			instance.compareAndSet(null, new ElectionManagement());
		}
		return instance.get();
	}

	public static ElectionManagement initElectionManagement(RoutingConf routingConf, NodeState nodeState) {
		ElectionManagement.routingConf = routingConf;
		ElectionManagement.raftStateMachine = new RaftStateMachine();
		electionTimer = new Timer();

		MessageBuilder.setRoutingConf(routingConf);
		instance.compareAndSet(null, new ElectionManagement(nodeState));
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
				incomingChannel.writeAndFlush(electionResponseMessage);
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
					} catch (Exception exception) {
						System.out.println("An error has occured while broadcasting the message.");
					}
				}
			}
		}

		if (electionWorkMessage.getRaftMessage().getAction() == ElectionAction.LEADER) {
			try {
				ElectionManagement electionManagement = instance.get();
				NodeState nodeState = new NodeState();
				nodeState.setLeaderId(electionWorkMessage.getRaftMessage().getLeaderId());
				nodeState.setLeaderHostId(electionWorkMessage.getRaftMessage().getLeaderHost());
				nodeState.setNodeState(State.NonLeader);

				electionManagement.setCurrentNodeState(nodeState);
				instance.compareAndSet(null, electionManagement);
				System.out.println("Current Leader ID : " + instance.get().getCurrentNodeState().getLeaderId());
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
			/*currentVoteCount++;
			if (currentVoteCount > ((NodeChannelManager.getNode2ChannelMap().size() / 2) + 1))
				return true;
			else
				return false;*/
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

	public NodeState getCurrentNodeState() {
		return currentNodeState;
	}

	public void setCurrentNodeState(NodeState currentNodeState) {
		this.currentNodeState = currentNodeState;
	}

}
