package com.redislabs.recharge.redis;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class SearchConfiguration extends AbstractRedisConfiguration {
	private boolean drop = false;
	private boolean create = true;
	private List<RediSearchField> schema = new ArrayList<>();
	private String language;
	private String score;
	private double defaultScore = 1d;
	private boolean replace;
	private boolean replacePartial;
	private boolean noSave;
}
