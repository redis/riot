package com.redis.riot.core;

import java.util.List;

import org.springframework.util.CollectionUtils;

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

	public boolean isEmpty() {
		return isEmptyIncludes() && isEmptyExcludes() && isEmptySlots();
	}

	public boolean isEmptySlots() {
		return CollectionUtils.isEmpty(slots);
	}

	public boolean isEmptyIncludes() {
		return CollectionUtils.isEmpty(includes);
	}

	public boolean isEmptyExcludes() {
		return CollectionUtils.isEmpty(excludes);
	}

}
