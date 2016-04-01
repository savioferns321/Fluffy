package gash.router.raft.leaderelection;

import java.util.Random;

public class RandomTimeoutGenerator {
	// Time in milliseconds
	private static int MINIMUM_TIMOUT = 1500;
	private static int MAXIMUM_TIMOUT = 4000;

	public static int randTimeout() {

		Random random = new Random();

		int randomNumber = random.nextInt(MAXIMUM_TIMOUT) + random.nextInt(MINIMUM_TIMOUT);

		return randomNumber;
	}
}
