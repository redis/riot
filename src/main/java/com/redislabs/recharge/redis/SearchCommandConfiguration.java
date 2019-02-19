package com.redislabs.recharge.redis;

import lombok.Data;

@Data
public class SearchCommandConfiguration {
	private String language;
	private String score;
	private double defaultScore = 1d;
	private boolean noSave;
}
