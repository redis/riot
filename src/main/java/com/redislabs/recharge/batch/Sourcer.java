package com.redislabs.recharge.batch;

import java.util.Map;

public interface Sourcer {

	Map<String, Object> getSource(Map<String, Object> in);
}
