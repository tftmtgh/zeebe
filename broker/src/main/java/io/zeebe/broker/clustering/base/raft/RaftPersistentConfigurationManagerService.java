/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.clustering.base.raft;

import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.system.configuration.DataCfg;
import io.zeebe.distributedlog.StorageConfiguration;
import io.zeebe.distributedlog.impl.LogstreamConfig;
import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.util.ByteValue;
import io.zeebe.util.FileUtil;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;

public class RaftPersistentConfigurationManagerService extends Actor {

  private static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;
  private static final String PARTITION_LOG_DIR = "segments";
  private static final String PARTITION_STATES_DIR = "state";

  private final List<StorageConfiguration> configurations = new ArrayList<>();

  private final int[] partitionCountPerDataDirectory;
  private final List<String> directories;
  private final String segmentSize;
  private final String maxFragmentSize;
  private final BrokerCfg configuration;

  public RaftPersistentConfigurationManagerService(final BrokerCfg configuration) {
    this.configuration = configuration;
    final DataCfg dataConfiguration = configuration.getData();

    for (final String directory : dataConfiguration.getDirectories()) {
      final File configDirectory = new File(directory);

      if (!configDirectory.exists()) {
        try {
          configDirectory.getParentFile().mkdirs();
          Files.createDirectory(configDirectory.toPath());
        } catch (final IOException e) {
          throw new RuntimeException("Unable to create directory " + configDirectory, e);
        }
      }
    }

    directories = configuration.getData().getDirectories();
    this.segmentSize = configuration.getData().getLogSegmentSize();
    this.maxFragmentSize = configuration.getNetwork().getMaxMessageSize().toString();
    partitionCountPerDataDirectory = new int[directories.size()];

    /* A temp solution so that DistributedLogstream primitive can create logs in this directory */
    LogstreamConfig.putConfig(String.valueOf(configuration.getCluster().getNodeId()), this);
  }

  @Override
  protected void onActorStarting() {
    for (int i = 0; i < directories.size(); i++) {
      readConfigurations(directories.get(i), i);
    }
  }

  private void readConfigurations(String dataDirectoryName, int offset) {
    final File dataDirectory = new File(dataDirectoryName);

    final File[] partitionDirectories =
        dataDirectory.listFiles((d, f) -> new File(d, f).isDirectory());

    for (File partitionDirectory : partitionDirectories) {
      final File logDirectory = new File(partitionDirectory, PARTITION_LOG_DIR);
      final File statesDirectory = new File(partitionDirectory, PARTITION_STATES_DIR);

      configurations.add(new StorageConfiguration(logDirectory, statesDirectory));
      partitionCountPerDataDirectory[offset]++;
    }
  }

  // get existing or create new
  public ActorFuture<StorageConfiguration> createConfiguration(int partitionId) {
    final ActorFuture<StorageConfiguration> future = new CompletableActorFuture<>();

    actor.run(
        () -> {
          final Optional<StorageConfiguration> partitionConfig =
              configurations.stream()
                  .filter((config) -> config.getPartitionId() == partitionId)
                  .findAny();
          if (partitionConfig.isPresent()) {
            future.complete(partitionConfig.get());
          } else {
            final String partitionName = String.format("partition-%d", partitionId);

            final int assignedDataDirOffset = assignDataDirectory();
            final String assignedDataDirectoryName = directories.get(assignedDataDirOffset);
            final File partitionDirectory = new File(assignedDataDirectoryName, partitionName);

            try {
              partitionDirectory.mkdir();

              final File logDirectory = new File(partitionDirectory, PARTITION_LOG_DIR);
              logDirectory.mkdir();

              final File statesDirectory = new File(partitionDirectory, PARTITION_STATES_DIR);
              statesDirectory.mkdir();

              final StorageConfiguration storage =
                  new StorageConfiguration(logDirectory, statesDirectory);

              storage
                  .setPartitionId(partitionId)
                  .setLogSegmentSize((int) new ByteValue(segmentSize).toBytes())
                  .setMaxFragmentSize((int) new ByteValue(maxFragmentSize).toBytes());

              configurations.add(storage);

              future.complete(storage);

              partitionCountPerDataDirectory[assignedDataDirOffset]++;
            } catch (Exception e) {
              try {
                // try to deleted partially created dirs / files
                FileUtil.deleteFolder(partitionDirectory.getAbsolutePath());
              } catch (IOException e1) {
                LOG.error("Unexpected error occurred on clean up.", e1);
              }

              future.completeExceptionally(e);
            }
          }
        });

    return future;
  }

  private int assignDataDirectory() {
    int min = Integer.MAX_VALUE;
    int minOffset = -1;

    for (int i = 0; i < partitionCountPerDataDirectory.length; i++) {
      final int partitionCount = partitionCountPerDataDirectory[i];

      if (partitionCount < min) {
        min = partitionCount;
        minOffset = i;
      }
    }

    return minOffset;
  }

  public ActorFuture<Void> close() {
    return actor.close();
  }
}
