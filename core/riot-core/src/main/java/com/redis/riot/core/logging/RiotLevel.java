package com.redis.riot.core.logging;

import java.util.logging.Level;

public class RiotLevel extends Level {

	/**
	 * Custom level between INFO and WARN
	 */
	public static final Level LIFECYCLE = new RiotLevel("LIFECYCLE", 850);

	private static final long serialVersionUID = 1L;

	public RiotLevel(String name, int value) {
		super(name, value);
	}

}