package com.redislabs.recharge;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.redislabs.recharge.file.FileType;
import com.redislabs.recharge.processor.ProcessorConfiguration;

import lombok.Data;
import lombok.ToString;

@Data
@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
@ToString
public class RechargeConfiguration {

	private Integer flushall;
	private boolean meter;
	private int chunkSize = 50;
	private ReaderConfiguration reader = new ReaderConfiguration();
	private ProcessorConfiguration processor;
	private WriterConfiguration writer = new WriterConfiguration();

	@SuppressWarnings("serial")
	private Map<String, FileType> fileTypes = new LinkedHashMap<String, FileType>() {
		{
			put("dat", FileType.Delimited);
			put("csv", FileType.Delimited);
			put("txt", FileType.FixedLength);
		}
	};

}
