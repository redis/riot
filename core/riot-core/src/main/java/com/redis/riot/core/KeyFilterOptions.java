package com.redis.riot.core;

import java.util.List;

public class KeyFilterOptions {

	private List<String> includes;
	private List<String> excludes;
	private List<SlotRange> slots;

	public List<String> getIncludes() {
		return includes;
	}

	public void setIncludes(List<String> patterns) {
		this.includes = patterns;
	}

	public List<String> getExcludes() {
		return excludes;
	}

	public void setExcludes(List<String> patterns) {
		this.excludes = patterns;
	}

	public List<SlotRange> getSlots() {
		return slots;
	}

	public void setSlots(List<SlotRange> ranges) {
		this.slots = ranges;
	}

}
