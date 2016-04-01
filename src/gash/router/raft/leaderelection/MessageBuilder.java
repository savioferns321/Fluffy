package gash.router.raft.leaderelection;

import gash.router.container.RoutingConf;
import pipe.common.Common.Header;
import pipe.election.Election.RaftMessage;
import pipe.election.Election.RaftMessage.ElectionAction;
import pipe.election.Election.VoteRequestMsg;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.StateOfLeader;

public class MessageBuilder {
	private static RoutingConf routingConf;

	public static WorkMessage buildElectionMessage() {
		WorkMessage.Builder workMessageBuilder = WorkMessage.newBuilder();
		Header.Builder headerBuilder = Header.newBuilder();
		RaftMessage.Builder raftMessageBuilder = RaftMessage.newBuilder();
		VoteRequestMsg.Builder voteRequestMsgBuilder = VoteRequestMsg.newBuilder();

		headerBuilder.setElection(true);
		headerBuilder.setNodeId(routingConf.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());

		voteRequestMsgBuilder.setCandidateId(routingConf.getNodeId());

		raftMessageBuilder.setAction(ElectionAction.REQUESTVOTE);

		raftMessageBuilder.setRequestVote(voteRequestMsgBuilder);
		workMessageBuilder.setStateOfLeader(StateOfLeader.LEADERUNKNOWN);
		workMessageBuilder.setRaftMessage(raftMessageBuilder);
		workMessageBuilder.setSecret(1234);
		workMessageBuilder.setHeader(headerBuilder);
		return workMessageBuilder.build();
	}

	public static WorkMessage buildElectionResponseMessage(int candidateId, boolean response) {
		WorkMessage.Builder workMessageBuilder = WorkMessage.newBuilder();
		Header.Builder headerBuilder = Header.newBuilder();
		RaftMessage.Builder raftMessageBuilder = RaftMessage.newBuilder();
		VoteRequestMsg.Builder voteRequestMsgBuilder = VoteRequestMsg.newBuilder();

		headerBuilder.setElection(true);
		headerBuilder.setNodeId(routingConf.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());

		voteRequestMsgBuilder.setCandidateId(candidateId);
		voteRequestMsgBuilder.setVoteGranted(response);

		raftMessageBuilder.setAction(ElectionAction.VOTE);
		raftMessageBuilder.setRequestVote(voteRequestMsgBuilder);

		workMessageBuilder.setStateOfLeader(StateOfLeader.LEADERUNKNOWN);
		workMessageBuilder.setRaftMessage(raftMessageBuilder);
		workMessageBuilder.setSecret(1234);
		workMessageBuilder.setHeader(headerBuilder);
		return workMessageBuilder.build();
	}

	public static WorkMessage buildLeaderResponseMessage(int leaderId) {
		WorkMessage.Builder workMessageBuilder = WorkMessage.newBuilder();
		Header.Builder headerBuilder = Header.newBuilder();
		RaftMessage.Builder raftMessageBuilder = RaftMessage.newBuilder();

		headerBuilder.setElection(true);
		headerBuilder.setNodeId(routingConf.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());

		raftMessageBuilder.setAction(ElectionAction.LEADER);
		raftMessageBuilder.setLeaderId(leaderId);
		raftMessageBuilder.setLeaderHost(HostAddressResolver.getLocalHostAddress());
		
		workMessageBuilder.setStateOfLeader(StateOfLeader.LEADERALIVE);
		workMessageBuilder.setRaftMessage(raftMessageBuilder);
		workMessageBuilder.setHeader(headerBuilder);
		workMessageBuilder.setSecret(1234);

		return workMessageBuilder.build();
	}

	public static RoutingConf getRoutingConf() {
		return routingConf;
	}

	public static void setRoutingConf(RoutingConf routingConf) {
		MessageBuilder.routingConf = routingConf;
	}

}
