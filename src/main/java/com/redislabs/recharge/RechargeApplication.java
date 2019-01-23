package com.redislabs.recharge;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.redislabs.recharge.batch.BatchConfiguration;
import com.redislabs.springredisearch.RediSearchConfiguration;

@SpringBootApplication(scanBasePackageClasses = { RechargeApplication.class, RediSearchConfiguration.class })
public class RechargeApplication implements ApplicationRunner {

	@Autowired
	private BatchConfiguration batch;

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(RechargeApplication.class, args);
		SpringApplication.exit(context);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		batch.runJob();
	}

}
