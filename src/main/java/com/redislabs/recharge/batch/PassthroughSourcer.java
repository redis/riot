package com.redislabs.recharge.batch;

import java.util.Map;

public class PassthroughSourcer implements Sourcer {

	@Override
	public Map<String, Object> getSource(Map<String, Object> in) {
		return in;
	}

}
