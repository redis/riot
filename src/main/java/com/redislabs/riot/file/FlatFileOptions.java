package com.redislabs.riot.file;

import org.springframework.batch.item.file.FlatFileItemReader;

import lombok.Data;

@Data
public class FlatFileOptions {

	private String[] names;
	private String encoding = FlatFileItemReader.DEFAULT_CHARSET;
	private int linesToSkip = 0;

}
