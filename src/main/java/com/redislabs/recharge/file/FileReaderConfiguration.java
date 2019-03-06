package com.redislabs.recharge.file;

import org.springframework.context.annotation.Configuration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Configuration
@Data
@EqualsAndHashCode(callSuper = true)
public class FileReaderConfiguration extends FileConfiguration {

	private DelimitedFileConfiguration delimited = new DelimitedFileConfiguration();
	private FixedLengthFileConfiguration fixedLength = new FixedLengthFileConfiguration();
	private JsonFileConfiguration json = new JsonFileConfiguration();

}
