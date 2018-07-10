package com.redislabs.recharge.config;

import lombok.Data;

@Data
public class FixedLengthConfiguration {

	String[] ranges;
	Boolean strict;

}
