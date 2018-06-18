package com.redislabs.recharge;

import org.ruaux.clic.AbstractApplication;
import org.ruaux.clic.ICommand;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RechargeApplication extends AbstractApplication {

	@Autowired
	private LoadCommand load;

	public static void main(String[] args) {
		SpringApplication.run(RechargeApplication.class, args);
	}

	@Override
	protected ICommand[] getCommands() {
		return new ICommand[] { load };
	}
}
