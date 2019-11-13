/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.transport.commandapi;

import io.zeebe.broker.PartitionChangeListener;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.transport.backpressure.PartitionAwareRequestLimiter;
import io.zeebe.broker.transport.backpressure.RequestLimiter;
import io.zeebe.engine.processor.CommandResponseWriter;
import io.zeebe.engine.processor.TypedRecord;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.intent.Intent;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceGroupReference;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.transport.Loggers;
import io.zeebe.transport.ServerOutput;
import io.zeebe.transport.ServerTransport;
import io.zeebe.transport.Transports;
import io.zeebe.transport.impl.memory.NonBlockingMemoryPool;
import io.zeebe.util.ByteValue;
import io.zeebe.util.sched.ActorScheduler;
import java.net.InetSocketAddress;
import java.util.function.Consumer;
import org.slf4j.Logger;

public class CommandApiService implements PartitionChangeListener {

  private final CommandApiMessageHandler service;
  private final PartitionAwareRequestLimiter limiter;
  public static final Logger LOG = Loggers.TRANSPORT_LOGGER;

  // max message size * factor = transport buffer size
  // - note that this factor is randomly chosen, feel free to change it
  private static final int TRANSPORT_BUFFER_FACTOR = 16;

  public CommandApiService(ActorScheduler scheduler,
      CommandApiMessageHandler commandApiMessageHandler, PartitionAwareRequestLimiter limiter,
      final String readableName,
    final InetSocketAddress bindAddress,
    final ByteValue maxMessageSize) {
    this.limiter = limiter;
    this.service = commandApiMessageHandler;

    final ByteValue transportBufferSize =
      ByteValue.ofBytes(maxMessageSize.toBytes() * TRANSPORT_BUFFER_FACTOR);

    ServerTransport serverTransport = Transports.newServerTransport()
      .name(readableName)
      .bindAddress(bindAddress)
      .scheduler(scheduler)
      .messageMemoryPool(new NonBlockingMemoryPool(transportBufferSize))
      .messageMaxLength(maxMessageSize)
      .build(commandApiMessageHandler, commandApiMessageHandler);

    LOG.info("Bound {} to {}", readableName, bindAddress);
  }

  @Override
  public void onNewLeaderPartition(int partitionId) {
    addPartition(partitionId);
  }

  @Override
  public void onNewFollowerPartition(int partitionId) {
    removePartition(partitionId);
  }

  public CommandResponseWriter newCommandResponseWriter() {
    return new CommandResponseWriterImpl(serverOutput);
  }

  private void removePartition(ServiceName<Partition> partitionServiceName, Partition partition) {
    limiter.removePartition(partition.getPartitionId());
    service.removePartition(partition.getLogStream());
  }

  private void addPartition(ServiceName<Partition> partitionServiceName, Partition partition) {
    limiter.addPartition(partition.getPartitionId());
    service.addPartition(partition.getLogStream(), limiter.getLimiter(partition.getPartitionId()));
  }

  public Consumer<TypedRecord> getOnProcessedListener(int partitionId) {
    final RequestLimiter<Intent> partitionLimiter = limiter.getLimiter(partitionId);
    return typedRecord -> {
      if (typedRecord.getRecordType() == RecordType.COMMAND && typedRecord.hasRequestMetadata()) {
        partitionLimiter.onResponse(typedRecord.getRequestStreamId(), typedRecord.getRequestId());
      }
    };
  }
}
