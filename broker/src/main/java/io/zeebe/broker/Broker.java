/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker;

import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.ATOMIX_JOIN_SERVICE;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.ATOMIX_SERVICE;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.PARTITIONS_BOOTSTRAP_SERVICE;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.RAFT_CONFIGURATION_MANAGER;
import static io.zeebe.broker.transport.TransportServiceNames.COMMAND_API_SERVER_NAME;

import io.atomix.core.Atomix;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.prometheus.client.CollectorRegistry;
import io.zeebe.broker.clustering.base.EmbeddedGatewayService;
import io.zeebe.broker.clustering.base.gossip.AtomixService;
import io.zeebe.broker.clustering.base.partitions.BootstrapPartitions;
import io.zeebe.broker.clustering.base.raft.RaftPersistentConfigurationManagerService;
import io.zeebe.broker.clustering.base.topology.NodeInfo;
import io.zeebe.broker.clustering.base.topology.TopologyManagerService;
import io.zeebe.broker.engine.impl.SubscriptionApiCommandMessageHandlerService;
import io.zeebe.broker.system.SystemContext;
import io.zeebe.broker.system.configuration.BackpressureCfg;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.system.configuration.NetworkCfg;
import io.zeebe.broker.system.configuration.SocketBindingCfg;
import io.zeebe.broker.system.management.LeaderManagementRequestHandler;
import io.zeebe.broker.system.monitoring.BrokerHealthCheckService;
import io.zeebe.broker.system.monitoring.BrokerHttpServer;
import io.zeebe.broker.transport.backpressure.PartitionAwareRequestLimiter;
import io.zeebe.broker.transport.commandapi.CommandApiMessageHandler;
import io.zeebe.broker.transport.commandapi.CommandApiService;
import io.zeebe.distributedlog.DistributedLogstream;
import io.zeebe.distributedlog.DistributedLogstreamBuilder;
import io.zeebe.distributedlog.DistributedLogstreamType;
import io.zeebe.distributedlog.impl.DistributedLogstreamConfig;
import io.zeebe.distributedlog.impl.DistributedLogstreamName;
import io.zeebe.distributedlog.impl.LogstreamConfig;
import io.zeebe.servicecontainer.CompositeServiceBuilder;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.LogUtil;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.clock.ActorClock;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;

public class Broker implements AutoCloseable {

  private static final CollectorRegistry METRICS_REGISTRY = CollectorRegistry.defaultRegistry;
  public static final Logger LOG = Loggers.SYSTEM_LOGGER;

  public static final String VERSION;

  static {
    final String version = Broker.class.getPackage().getImplementationVersion();
    VERSION = version != null ? version : "development";
  }

  protected final SystemContext brokerContext;
  protected boolean isClosed = false;

  public Broker(final String configFileLocation, final String basePath, final ActorClock clock) {
    this(new SystemContext(configFileLocation, basePath, clock));
  }

  public Broker(final InputStream configStream, final String basePath, final ActorClock clock) {
    this(new SystemContext(configStream, basePath, clock));
  }

  public Broker(final BrokerCfg cfg, final String basePath, final ActorClock clock) {
    this(new SystemContext(cfg, basePath, clock));
  }

  public Broker(final SystemContext systemContext) {
    this.brokerContext = systemContext;
    LogUtil.doWithMDC(systemContext.getDiagnosticContext(), () -> start());
  }

  private final List<PartitionChangeListener> partitionChangeListeners = new ArrayList<>();

  protected void start() {
    if (LOG.isInfoEnabled()) {
      LOG.info("Version: {}", VERSION);
      LOG.info("Starting broker with configuration {}", getConfig().toJson());
    }

    final ActorScheduler scheduler = brokerContext.getScheduler();

    ////////////////////////////////////////////////////////////////////////
    // RAFT STORAGE - DO WE NEED THAT?
    ////////////////////////////////////////////////////////////////////////
    final BrokerCfg brokerCfg = brokerContext.getBrokerConfiguration();
    final RaftPersistentConfigurationManagerService raftConfigurationManagerService =
        new RaftPersistentConfigurationManagerService(brokerCfg);

    // dirs have to be created
    scheduler.submitActor(raftConfigurationManagerService).join();

    ////////////////////////////////////////////////////////////////////////
    // ATOMIX
    ////////////////////////////////////////////////////////////////////////
    final AtomixService atomixService = new AtomixService(brokerCfg);
    final Atomix atomix = atomixService.getAtomix();

    if (brokerCfg.getGateway().isEnable()) {
      new EmbeddedGatewayService(brokerCfg, scheduler, atomix).startGateway();
    }
    // when all added we need to set the listeners to the atomix service - to be called

    ////////////////////////////////////////////////////////////////////////
    // HEALTH CHECK
    ////////////////////////////////////////////////////////////////////////
    final BrokerHealthCheckService healthCheckService = new BrokerHealthCheckService(atomix);
    partitionChangeListeners.add(healthCheckService);
    scheduler.submitActor(healthCheckService).join();
    final SocketBindingCfg monitoringApi = brokerCfg.getNetwork().getMonitoringApi();
    new BrokerHttpServer(
      monitoringApi.getHost(), monitoringApi.getPort(), METRICS_REGISTRY, healthCheckService);


    ////////////////////////////////////////////////////////////////////////
    // PUSH DEPLOYMENT STUFF - MAYBE there is an other way?
    ////////////////////////////////////////////////////////////////////////
    // todo fix partition listener this guy needs somehow logstream
    // merge with leader partition ?!
    // or give partition and this guys calls partition#writeNewEvent when follower it does nothing
    // in that case no need to know who is follower?
    // or follower writes rejection !
    final LeaderManagementRequestHandler requestHandlerService =
        new LeaderManagementRequestHandler(atomix);
    scheduler.submitActor(requestHandlerService);
    partitionChangeListeners.add(requestHandlerService);

    ////////////////////////////////////////////////////////////////////////
    // COMMAND API
    ////////////////////////////////////////////////////////////////////////
    //    brokerContext.addComponent(new TransportComponent());
    final CommandApiMessageHandler commandApiMessageHandler = new CommandApiMessageHandler();
    final BackpressureCfg backpressure = brokerCfg.getBackpressure();
    PartitionAwareRequestLimiter limiter = PartitionAwareRequestLimiter.newNoopLimiter();
    if (backpressure.isEnabled()) {
      limiter = PartitionAwareRequestLimiter.newLimiter(
        backpressure.getAlgorithm(), backpressure.useWindowed());
    }
    final NetworkCfg networkCfg = brokerCfg.getNetwork();
    final SocketAddress bindAddr = networkCfg.getCommandApi().getAddress();
    final CommandApiService commandHandler =
      new CommandApiService(scheduler, commandApiMessageHandler, limiter, COMMAND_API_SERVER_NAME, bindAddr.toInetSocketAddress(), networkCfg.getMaxMessageSize());
    partitionChangeListeners.add(commandHandler);


    ////////////////////////////////////////////////////////////////////////
    // SUBSCRIPTION API
    ////////////////////////////////////////////////////////////////////////
    //    brokerContext.addComponent(new EngineComponent());
    final SubscriptionApiCommandMessageHandlerService messageHandlerService =
      new SubscriptionApiCommandMessageHandlerService(atomix);
    scheduler.submitActor(messageHandlerService);
    partitionChangeListeners.add(messageHandlerService);

    //    brokerContext.addComponent(new ClusterComponent());

    final NodeInfo localMember =
        new NodeInfo(
            brokerCfg.getCluster().getNodeId(),
            networkCfg.getCommandApi().getAdvertisedAddress());

    /* A hack so that DistributedLogstream primitive can create logstream services using this serviceContainer */
    // todo investigate whether we need this?!
    LogstreamConfig.putServiceContainer(
        String.valueOf(localMember.getNodeId()), context.getServiceContainer());

    // DO WE STILL NEED THIS?
    final TopologyManagerService topologyManagerService =
        new TopologyManagerService(localMember, brokerCfg.getCluster(), atomix);
    scheduler.submitActor(topologyManagerService);
    partitionChangeListeners.add(topologyManagerService);


    ////////////////////////////////////////////////////////////////////////
    // ATOMIX JOIN AND PRIMITIVE
    ////////////////////////////////////////////////////////////////////////
    //final AtomixJoinService atomixJoinService = new AtomixJoinService(atomix);
    // todo add listeners before to atomix
    // then join !
    atomix.start().join();

    // Create distributed log primitive. No need to wait until the partitions are created.
    // TODO: Move it somewhere else. Only one node has to create it.
    final MultiRaftProtocol PROTOCOL =
      MultiRaftProtocol.builder()
        // Maps partitionName to partitionId
        .withPartitioner(DistributedLogstreamName.getInstance())
        .build();
    final String primitiveName = "distributed-log";
    final CompletableFuture<DistributedLogstream> distributedLogstreamCompletableFuture =
      atomix
        .<DistributedLogstreamBuilder, DistributedLogstreamConfig, DistributedLogstream>
          primitiveBuilder(primitiveName, DistributedLogstreamType.instance())
        .withProtocol(PROTOCOL)
        .buildAsync();

    ////////////////////////////////////////////////////////////////////////
    // Partition bootstrap should maybe done in primitive?!
    ////////////////////////////////////////////////////////////////////////
    final BootstrapPartitions partitionBootstrapService =
      new BootstrapPartitions(context.getBrokerConfiguration());
    context
      .getServiceContainer()
      .createService(PARTITIONS_BOOTSTRAP_SERVICE, partitionBootstrapService)
      .dependency(ATOMIX_SERVICE, partitionBootstrapService.getAtomixInjector())
      .dependency(ATOMIX_JOIN_SERVICE)
      .dependency(
        RAFT_CONFIGURATION_MANAGER, partitionBootstrapService.getConfigurationManagerInjector())
      .install();


    brokerContext.init();
  }

  @Override
  public void close() {
    LogUtil.doWithMDC(
        brokerContext.getDiagnosticContext(),
        () -> {
          if (!isClosed) {
            brokerContext.close();
            isClosed = true;
            LOG.info("Broker shut down.");
          }
        });
  }

  public SystemContext getBrokerContext() {
    return brokerContext;
  }

  public BrokerCfg getConfig() {
    return brokerContext.getBrokerConfiguration();
  }
}
