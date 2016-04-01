package gash.router.raft.leaderelection;

public interface StateMachine {
	
    boolean isFollower();

    void becomeFollower();

    void becomeCandidate();

    ElectionState getState();

    boolean isLeader();

    void becomeLeader();

    boolean isCandidate();
}
