package com.redislabs.recharge.batch;

import java.util.List;
import java.util.Map;

import org.springframework.batch.core.ItemWriteListener;

public class MeteredItemWriteListener implements ItemWriteListener<Map<String, Object>> {

	private MeteringProvider metering;
	private String writerName;
	private String tagName;

	public MeteredItemWriteListener(String writerName, String tagName, MeteringProvider metering) {
		this.writerName = writerName;
		this.tagName = tagName;
		this.metering = metering;
	}

	@Override
	public void beforeWrite(List<? extends Map<String, Object>> items) {
		metering.startTimer(writerName, tagName);
	}

	@Override
	public void afterWrite(List<? extends Map<String, Object>> items) {
		metering.stopTimer(writerName, tagName);
	}

	@Override
	public void onWriteError(Exception exception, List<? extends Map<String, Object>> items) {
		// TODO Auto-generated method stub

	}

}
