package com.redis.riot.gen;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class FakerReaderTestApplication {

	public static void main(String[] args) {
		SpringApplication.run(FakerReaderTestApplication.class, args);
	}

}