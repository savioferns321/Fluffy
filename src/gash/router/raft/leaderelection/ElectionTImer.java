package gash.router.raft.leaderelection;

public class ElectionTImer implements Runnable {

	@Override
	public void run() {
		/*
		 * ElectionManagement electionManagementInstance =
		 * ElectionManagement.getInstance(); if (electionManagementInstance !=
		 * null) { NodeState currentNodeState =
		 * electionManagementInstance.getCurrentNodeState(); if
		 * (currentNodeState != null) { System.out.println(
		 * "Current Node State Leader ID " + currentNodeState.getLeaderId()); }
		 * } else { System.out.println("ElectionManagement is Null "); }
		 */

		ElectionManagement.resetElection();
		ElectionManagement.startElection();
	}

}

/*
 * public class ElectionTImer extends TimerTask {
 * 
 * @Override public void run() {
 * 
 * ElectionManagement electionManagementInstance =
 * ElectionManagement.getInstance(); if (electionManagementInstance != null) {
 * NodeState currentNodeState =
 * electionManagementInstance.getCurrentNodeState(); if (currentNodeState !=
 * null) { System.out.println( "Current Node State Leader ID " +
 * currentNodeState.getLeaderId()); } } else { System.out.println(
 * "ElectionManagement is Null "); }
 * 
 * ElectionManagement.resetElection(); ElectionManagement.startElection(); }
 * 
 * }
 */