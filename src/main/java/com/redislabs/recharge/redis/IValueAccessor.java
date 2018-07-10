package com.redislabs.recharge.redis;

import java.util.Map;

public interface IValueAccessor {

	Object[] getValueArray(Map<String, Object> map);

}
