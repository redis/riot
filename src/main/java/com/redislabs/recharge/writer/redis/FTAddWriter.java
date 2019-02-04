package com.redislabs.recharge.writer.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings({ "rawtypes", "unchecked" })
public class FTAddWriter extends AbstractSearchWriter {

	public FTAddWriter(RediSearchClient client, RedisWriterConfiguration writer) {
		super(client, writer);
	}

	@Override
	protected void write(String key, Map record) {
		double score = getScore(record);
		AddOptions options = AddOptions.builder().noSave(config.getSearch().isNoSave())
				.replace(config.getSearch().isReplace()).replacePartial(config.getSearch().isReplacePartial()).build();
		try {
			commands.add(config.getKeyspace(), key, score, convert(record), options);
		} catch (Exception e) {
			if ("Document already exists".equals(e.getMessage())) {
				log.debug(e.getMessage());
			} else {
				log.error("Could not add document: {}", e.getMessage());
			}
		}
	}

	protected double getScore(Map<String, Object> record) {
		if (config.getSearch().getScore() == null) {
			return config.getSearch().getDefaultScore();
		}
		return convert(record.get(config.getSearch().getScore()), Double.class);
	}

}
