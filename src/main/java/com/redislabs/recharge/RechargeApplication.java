package com.redislabs.recharge;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RechargeApplication implements ApplicationRunner {

	@Autowired
	private RechargeConfig config;

	public static void main(String[] args) {
		System.setProperty("spring.profiles.active", "default");
		System.setProperty("spring.jmx.enabled", "false");
		System.setProperty("logging.level.com.redislabs.recharge.RechargeApplication", "warn");
		System.setProperty("logging.level.org.springframework", "warn");
		System.setProperty("logging.level.io.lettuce.core.EpollProvider", "warn");
		System.setProperty("logging.level.io.lettuce.core.KqueueProvider", "warn");
		SpringApplication.exit(SpringApplication.run(RechargeApplication.class, args));
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		if (isExport(args)) {
			System.err.println("Export not yet supported");
		} else {
			config.runImport();
		}
	}

	private boolean isExport(ApplicationArguments args) {
		return !args.getNonOptionArgs().isEmpty() && args.getNonOptionArgs().get(0).equals("export");
	}

}
