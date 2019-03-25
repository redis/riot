package com.redislabs.recharge;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
public class RechargeApplication implements ApplicationRunner {

	public static void main(String[] args) {
		SpringApplication.exit(SpringApplication.run(RechargeApplication.class, args));
	}

	@Autowired
	@Qualifier("importJob")
	Job importJob;

	@Autowired
	JobLauncher jobLauncher;

	@Override
	public void run(ApplicationArguments args) throws Exception {
		if (isExport(args)) {
			log.warn("Export not yet supported");
		} else {
			jobLauncher.run(importJob, new JobParameters());
		}
	}

	private boolean isExport(ApplicationArguments args) {
		return !args.getNonOptionArgs().isEmpty() && args.getNonOptionArgs().get(0).equals("export");
	}

}
