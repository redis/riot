package com.redis.riot.core;

public class SlotRange {

	private final int start;
	private final int end;

	public SlotRange(int start, int end) {
		this.start = start;
		this.end = end;
	}

	public int getStart() {
		return start;
	}

	public int getEnd() {
		return end;
	}

	public static SlotRange of(int start, int end) {
		return new SlotRange(start, end);
	}

}