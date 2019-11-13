/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.engine.impl;

import io.atomix.core.Atomix;
import io.zeebe.broker.PartitionChangeListener;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.engine.processor.workflow.message.command.SubscriptionCommandMessageHandler;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceGroupReference;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.Actor;
import org.agrona.collections.Int2ObjectHashMap;

public class SubscriptionApiCommandMessageHandlerService extends Actor
    implements PartitionChangeListener {

  private final Int2ObjectHashMap<LogStream> leaderPartitions = new Int2ObjectHashMap<>();
  private SubscriptionCommandMessageHandler messageHandler;
  private Atomix atomix;

  public SubscriptionApiCommandMessageHandlerService(Atomix atomix) {
    this.atomix = atomix;
  }

  @Override
  public String getName() {
    return "subscription-api";
  }

  @Override
  protected void onActorStarting() {
    messageHandler = new SubscriptionCommandMessageHandler(actor::call, leaderPartitions::get);
    atomix.getCommunicationService().subscribe("subscription", messageHandler);
  }

  @Override
  public void onNewLeaderPartition(int partitionId) {
    addPartition(partitionId);
  }

  @Override
  public void onNewFollowerPartition(int partitionId) {
    removePartition(partitionId);
  }

  private void addPartition(final ServiceName<Partition> sericeName, final Partition partition) {
    actor.submit(() -> leaderPartitions.put(partition.getPartitionId(), partition.getLogStream()));
  }

  private void removePartition(final ServiceName<Partition> sericeName, final Partition partition) {
    actor.submit(() -> leaderPartitions.remove(partition.getPartitionId()));
  }
}
