package com.redislabs.recharge;

import java.util.Arrays;
import java.util.List;

import org.springframework.batch.core.Job;
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

	private static final String IMPORT = "import";
	private static final String EXPORT = "export";

	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private BatchConfig batch;
	@Autowired
	private RediSearchClient client;
	@Autowired
	private RechargeConfiguration config;

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
		nonOptionArgs(args).forEach(arg -> {
			try {
				jobLauncher.run(job(arg), new JobParameters());
			} catch (Exception e) {
				log.error("Could not run command {}", arg, e);
			}
		});

	}

	private Job job(String command) throws RechargeException {
		log.info("{} {}", command, config);
		switch (command) {
		case EXPORT:
			return batch.exportJob();
		default:
			return batch.importJob();
		}

	}

	private List<String> nonOptionArgs(ApplicationArguments args) {
		if (args.getNonOptionArgs().isEmpty()) {
			return Arrays.asList(IMPORT);
		}
		return args.getNonOptionArgs();
	}

}
