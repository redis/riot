package com.redislabs.riot.file;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class FixedLengthFileOptions extends FlatFileOptions {

	private String[] ranges = new String[0];

}
