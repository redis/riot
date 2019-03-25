package com.redislabs.recharge.generator;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

@ConfigurationProperties(prefix = "generator")
@Data
public class GeneratorProperties {

	private String expression;
	private List<GeneratorField> fields = new ArrayList<>();
	private String locale = "en-US";
	private int max = 1000;

}