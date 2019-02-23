package com.redislabs.recharge.redis.search;

import lombok.Data;

@Data
public class Field {
	private String name;
	private boolean sortable;
	private boolean noIndex;
}
