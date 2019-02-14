package com.redislabs.recharge;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.springredisearch.RediSearchConfiguration;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication(scanBasePackageClasses = { RechargeApplication.class, RediSearchConfiguration.class })
@Slf4j
public class RechargeApplication implements ApplicationRunner {
	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private BatchConfig batch;
	@Autowired
	private RechargeConfiguration config;
	@Autowired
	private RediSearchClient client;

	public static void main(String[] args) {
		SpringApplication.exit(SpringApplication.run(RechargeApplication.class, args));
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		if (config.isFlushall()) {
			log.warn("Flushing database in {} seconds", config.getFlushallWait());
			Thread.sleep(config.getFlushallWait() * 1000);
			client.connect().sync().flushall();
		}
		jobLauncher.run(batch.job(), new JobParameters());
	}

}
