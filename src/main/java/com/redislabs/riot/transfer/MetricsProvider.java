package com.redislabs.riot.transfer;

import java.util.stream.Collectors;

public interface MetricsProvider {

    Metrics getMetrics();

}
