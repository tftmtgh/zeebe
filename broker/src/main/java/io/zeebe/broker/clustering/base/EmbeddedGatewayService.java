/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.clustering.base;

import io.atomix.core.Atomix;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.gateway.Gateway;
import io.zeebe.gateway.impl.broker.BrokerClient;
import io.zeebe.gateway.impl.broker.BrokerClientImpl;
import io.zeebe.gateway.impl.configuration.GatewayCfg;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.ActorScheduler;
import java.io.IOException;
import java.util.function.Function;

public class EmbeddedGatewayService {

  private final Gateway gateway;

  public EmbeddedGatewayService(BrokerCfg configuration, ActorScheduler scheduler, Atomix atomix) {
    final Function<GatewayCfg, BrokerClient> brokerClientFactory =
        cfg -> new BrokerClientImpl(cfg, atomix, scheduler, false);
    gateway = new Gateway(configuration.getGateway(), brokerClientFactory, scheduler);
  }

  public void stop(ServiceStopContext stopContext) {
    if (gateway != null) {
      stopContext.run(gateway::stop);
    }
  }

  public Gateway getGateway() {
    return gateway;
  }

  public void startGateway() {
    try {
      gateway.start();
    } catch (final IOException e) {
      throw new RuntimeException("Gateway was not able to start", e);
    }
  }
}
