package com.redislabs.recharge.file;

import org.springframework.batch.item.file.FlatFileItemReader;

import lombok.Data;

@Data
public class FlatFileConfiguration {
	private String encoding = FlatFileItemReader.DEFAULT_CHARSET;
	private boolean header = true;
	private int linesToSkip = 0;
}
