package com.redislabs.recharge.redis.search.add;

import java.util.ArrayList;
import java.util.List;

import com.redislabs.recharge.redis.search.RediSearchCommandConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class AddConfiguration extends RediSearchCommandConfiguration {
	private boolean drop = false;
	private boolean dropKeepDocs = false;
	private boolean create = true;
	private String[] keys = new String[0];
	private List<SchemaField> schema = new ArrayList<>();
	private String language;
	private String score;
	private double defaultScore = 1d;
	private boolean replace;
	private boolean replacePartial;
	private boolean noSave;
}
