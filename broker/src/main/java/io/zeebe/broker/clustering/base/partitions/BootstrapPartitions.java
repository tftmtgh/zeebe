/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.clustering.base.partitions;

import static io.zeebe.broker.clustering.base.partitions.Partition.getPartitionName;
import static io.zeebe.broker.clustering.base.partitions.PartitionServiceNames.partitionInstallServiceName;

import io.atomix.cluster.MemberId;
import io.atomix.core.Atomix;
import io.atomix.protocols.raft.partition.RaftPartition;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.zeebe.broker.clustering.base.raft.RaftPersistentConfigurationManagerService;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.distributedlog.StorageConfiguration;
import io.zeebe.distributedlog.StorageConfigurationManager;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.util.sched.ActorScheduler;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Always installed on broker startup: reads configuration of all locally available partitions and
 * starts the corresponding services (logstream, partition ...)
 */
public class BootstrapPartitions {
  private final BrokerCfg brokerCfg;
  private final RaftPersistentConfigurationManagerService raftCfgManager;
  private final Atomix atomix;
  private final ActorScheduler actorScheduler;

  public BootstrapPartitions(final BrokerCfg brokerCfg, ActorScheduler actorScheduler, Atomix atomix, RaftPersistentConfigurationManagerService raftPersistentConfigurationManagerService) {
    this.brokerCfg = brokerCfg;
    this.atomix = atomix;
    this.actorScheduler = actorScheduler;
    this.raftCfgManager = raftPersistentConfigurationManagerService;

    final RaftPartitionGroup partitionGroup =
        (RaftPartitionGroup) atomix.getPartitionService().getPartitionGroup(Partition.GROUP_NAME);

    final MemberId nodeId = atomix.getMembershipService().getLocalMember().id();
    final List<RaftPartition> owningPartitions =
        partitionGroup.getPartitions().stream()
            .filter(partition -> partition.members().contains(nodeId))
            .map(RaftPartition.class::cast)
            .collect(Collectors.toList());

          for (RaftPartition owningPartition : owningPartitions) {
            installPartition(owningPartition);
          }
  }


  private void installPartition(RaftPartition partition) {
    final StorageConfiguration configuration =
        raftCfgManager.createConfiguration(partition.id().id()).join();
    final String partitionName = getPartitionName(configuration.getPartitionId());
    final ServiceName<PartitionInstallService> partitionInstallServiceName =
        partitionInstallServiceName(partitionName);

    final PartitionInstallService partitionInstallService =
        new PartitionInstallService(
            partition,
            atomix.getEventService(),
            atomix.getCommunicationService(),
            configuration,
            brokerCfg);

    actorScheduler.submitActor(partitionInstallService);
  }

}
