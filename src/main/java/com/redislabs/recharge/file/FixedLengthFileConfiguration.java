package com.redislabs.recharge.file;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class FixedLengthFileConfiguration extends FlatFileConfiguration {
	private String[] ranges = new String[0];
	private Boolean strict;
}