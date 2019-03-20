package com.redislabs.recharge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RechargeApplication {

//	@Autowired
//	private JobLauncher jobLauncher;
//	@Autowired
//	private BatchConfig batch;
//
	public static void main(String[] args) {
		SpringApplication.exit(SpringApplication.run(RechargeApplication.class, args));
	}
//
//	@Override
//	public void run(ApplicationArguments args) throws Exception {
//		jobLauncher.run(batch.job(), new JobParameters());
//	}

}
