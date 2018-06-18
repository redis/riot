package com.redislabs.recharge;

import org.ruaux.clic.Command;
import org.ruaux.clic.ICommand;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Component;

@Component
@Command("load")
public class LoadCommand implements ICommand {

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private Job loadJob;

	@Override
	public void execute(ApplicationArguments args) {
		try {
			jobLauncher.run(loadJob, new JobParameters());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
