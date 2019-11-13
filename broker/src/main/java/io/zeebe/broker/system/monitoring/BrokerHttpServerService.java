/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.monitoring;

import io.prometheus.client.CollectorRegistry;

public class BrokerHttpServerService implements AutoCloseable {

  private final BrokerHttpServer brokerHttpServer;

  public BrokerHttpServerService(
      String host,
      int port,
      CollectorRegistry metricsRegistry,
      BrokerHealthCheckService brokerHealthCheckService) {
    brokerHttpServer = new BrokerHttpServer(host, port, metricsRegistry, brokerHealthCheckService);
  }

  @Override
  public void close() {
    brokerHttpServer.close();
  }
}
