package com.redislabs.recharge.redis;

import java.util.Map;

public class FirstValueAccessor implements IValueAccessor {

	@Override
	public Object[] getValueArray(Map<String, Object> map) {
		return new Object[] { map.values().iterator().next() };
	}

}
