package com.redis.riot.core;

import java.util.List;

import com.redis.spring.batch.common.Range;

public class KeyFilterOptions {

	private List<String> includes;
	private List<String> excludes;
	private List<Range> slots;

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

	public List<Range> getSlots() {
		return slots;
	}

	public void setSlots(List<Range> ranges) {
		this.slots = ranges;
	}

}
