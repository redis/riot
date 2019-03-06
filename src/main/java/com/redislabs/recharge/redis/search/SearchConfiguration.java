package com.redislabs.recharge.redis.search;

import java.util.ArrayList;
import java.util.List;

import com.redislabs.recharge.redis.DataStructureConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class SearchConfiguration extends DataStructureConfiguration {
	private boolean drop = false;
	private boolean dropKeepDocs = false;
	private boolean create = true;
	private List<SearchField> schema = new ArrayList<>();
	private String language;
	private String score;
	private double defaultScore = 1d;
	private boolean replace;
	private boolean replacePartial;
	private boolean noSave;
}
