package com.linkedin.venice.ingestion.control;

import static com.linkedin.venice.meta.BufferReplayPolicy.REWIND_FROM_EOP;
import static org.mockito.Mockito.*;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.PartitionInfo;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RealTimeTopicSwitcherTest {
  private static final int KAFKA_RF_FOR_RT_TOPICS = 6;
  private static final int KAFKA_MIN_ISR_FOR_RT_TOPICS = 4;
  private RealTimeTopicSwitcher leaderStorageNodeReplicator;
  private TopicManager mockTopicManager;
  private VeniceWriterFactory mockVeniceWriterFactory;
  private String aggregateRealTimeSourceKafkaUrl = "aggregate-real-time-source-kafka-url";

  @BeforeMethod
  public void setUp() {
    mockTopicManager = mock(TopicManager.class);
    mockVeniceWriterFactory = mock(VeniceWriterFactory.class);
    Properties properties = new Properties();
    properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, "dummy");
    properties.put(ConfigKeys.KAFKA_REPLICATION_FACTOR, "3");
    properties.put(ConfigKeys.KAFKA_REPLICATION_FACTOR_RT_TOPICS, Integer.toString(KAFKA_RF_FOR_RT_TOPICS));
    properties.put(ConfigKeys.KAFKA_MIN_IN_SYNC_REPLICAS_RT_TOPICS, Integer.toString(KAFKA_MIN_ISR_FOR_RT_TOPICS));
    // filler bootstrap servers
    leaderStorageNodeReplicator =
        new RealTimeTopicSwitcher(mockTopicManager, mockVeniceWriterFactory, new VeniceProperties(properties));
  }

  @Test
  public void testPrepareAndStartReplication() {
    String srcTopic = "testTopic_rt";
    String destTopic = "testTopic_v1";
    Store mockStore = mock(Store.class);
    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);
    List<PartitionInfo> partitionInfos = new ArrayList<>();
    VeniceWriter<byte[], byte[], byte[]> mockVeniceWriter = mock(VeniceWriter.class);

    doReturn(true).when(mockStore).isHybrid();
    doReturn(mockHybridConfig).when(mockStore).getHybridStoreConfig();
    Version version = new VersionImpl(Version.parseStoreFromKafkaTopicName(destTopic), 1, "test-id");
    doReturn(Optional.of(version)).when(mockStore).getVersion(Version.parseVersionFromKafkaTopicName(destTopic));
    doReturn(3600L).when(mockHybridConfig).getRewindTimeInSeconds();
    doReturn(REWIND_FROM_EOP).when(mockHybridConfig).getBufferReplayPolicy();
    doReturn(true).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(srcTopic);
    doReturn(true).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(destTopic);
    doReturn(partitionInfos).when(mockTopicManager).partitionsFor(srcTopic);
    doReturn(partitionInfos).when(mockTopicManager).partitionsFor(destTopic);
    doReturn(mockVeniceWriter).when(mockVeniceWriterFactory)
        .createBasicVeniceWriter(anyString(), any(Time.class), any(VenicePartitioner.class), anyInt());

    leaderStorageNodeReplicator.switchToRealTimeTopic(
        srcTopic,
        destTopic,
        mockStore,
        aggregateRealTimeSourceKafkaUrl,
        Collections.emptyList());

    verify(mockVeniceWriter).broadcastTopicSwitch(any(), anyString(), anyLong(), any());
  }

  @Test
  public void testPrepareAndStartReplicationWithNativeReplication() {
    String srcTopic = "testTopic_rt";
    String destTopic = "testTopic_v1";
    Store mockStore = mock(Store.class);
    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);
    List<PartitionInfo> partitionInfos = new ArrayList<>();
    VeniceWriter<byte[], byte[], byte[]> mockVeniceWriter = mock(VeniceWriter.class);

    doReturn(true).when(mockStore).isHybrid();
    doReturn(mockHybridConfig).when(mockStore).getHybridStoreConfig();
    Version version = new VersionImpl(Version.parseStoreFromKafkaTopicName(destTopic), 1, "test-id");
    version.setNativeReplicationEnabled(true);
    doReturn(Optional.of(version)).when(mockStore).getVersion(Version.parseVersionFromKafkaTopicName(destTopic));
    doReturn(3600L).when(mockHybridConfig).getRewindTimeInSeconds();
    doReturn(REWIND_FROM_EOP).when(mockHybridConfig).getBufferReplayPolicy();
    doReturn(DataReplicationPolicy.AGGREGATE).when(mockHybridConfig).getDataReplicationPolicy();
    doReturn(true).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(srcTopic);
    doReturn(true).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(destTopic);
    doReturn(partitionInfos).when(mockTopicManager).partitionsFor(srcTopic);
    doReturn(partitionInfos).when(mockTopicManager).partitionsFor(destTopic);
    doReturn(mockVeniceWriter).when(mockVeniceWriterFactory)
        .createBasicVeniceWriter(anyString(), any(Time.class), any(VenicePartitioner.class), anyInt());

    leaderStorageNodeReplicator.switchToRealTimeTopic(
        srcTopic,
        destTopic,
        mockStore,
        aggregateRealTimeSourceKafkaUrl,
        Collections.emptyList());

    List<CharSequence> expectedSourceClusters = new ArrayList<>();
    expectedSourceClusters.add(aggregateRealTimeSourceKafkaUrl);
    verify(mockVeniceWriter).broadcastTopicSwitch(eq(expectedSourceClusters), eq(srcTopic), anyLong(), any());
  }

  @Test
  public void testSendVersionSwap() {
    String storeName = "TestStore";
    Map<String, ViewConfig> viewConfigs = new HashMap<>();
    viewConfigs.put("testView", new ViewConfigImpl("testClass", Collections.emptyMap()));

    Version version1 = new VersionImpl(storeName, 1, "push1");
    Version version2 = new VersionImpl(storeName, 2, "push2");
    version2.setViewConfigs(viewConfigs);
    Version version3 = new VersionImpl(storeName, 3, "push3");
    version3.setViewConfigs(viewConfigs);

    String realTimeTopic = Version.composeRealTimeTopic(storeName);
    Store mockStore = mock(Store.class);
    when(mockStore.getName()).thenReturn(storeName);
    when(mockStore.getVersion(1)).thenReturn(Optional.of(version1));
    when(mockStore.getVersion(2)).thenReturn(Optional.of(version2));
    when(mockStore.getVersion(3)).thenReturn(Optional.of(version3));
    when(mockStore.getVersion(4)).thenReturn(Optional.empty());

    TopicManager mockTopicManager = mock(TopicManager.class);
    when(mockTopicManager.containsTopic(realTimeTopic)).thenReturn(true);

    VeniceWriter mockVeniceWriter = mock(VeniceWriter.class);

    VeniceWriterFactory mockWriterFactory = mock(VeniceWriterFactory.class);
    ArgumentCaptor<String> realTimeTopicCaptor = ArgumentCaptor.forClass(String.class);
    when(mockWriterFactory.createBasicVeniceWriter(anyString(), any(), any(), anyInt())).thenReturn(mockVeniceWriter);

    VeniceProperties mockVeniceProperties = mock(VeniceProperties.class);
    when(mockVeniceProperties.getString(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS))
        .thenReturn(aggregateRealTimeSourceKafkaUrl);

    RealTimeTopicSwitcher realTimeTopicSwitcher =
        new RealTimeTopicSwitcher(mockTopicManager, mockWriterFactory, mockVeniceProperties);

    // Version 1 does not have a view but 2 does. In this case DON'T transmit a version swap message
    realTimeTopicSwitcher.transmitVersionSwapMessage(mockStore, 1, 2);
    verify(mockWriterFactory, never()).createVeniceWriter(anyString(), any(), any(), anyInt());

    // Version 4 doesn't exist. In this case DON'T transmit a version swap message, and throw an exception to boot
    Assert.assertThrows(() -> realTimeTopicSwitcher.transmitVersionSwapMessage(mockStore, 3, 4));

    // Version 2 and 3 both have view configs, so we should transmit a version swap message
    realTimeTopicSwitcher.transmitVersionSwapMessage(mockStore, 2, 3);
    verify(mockWriterFactory, times(2)).createBasicVeniceWriter(realTimeTopicCaptor.capture(), any(), any(), anyInt());
    Assert.assertEquals(realTimeTopicCaptor.getValue(), realTimeTopic);

    ArgumentCaptor<String> oldVersionCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> newVersionCaptor = ArgumentCaptor.forClass(String.class);

    // Verify version swap message arguments look right
    verify(mockVeniceWriter, times(2))
        .broadcastVersionSwap(oldVersionCaptor.capture(), newVersionCaptor.capture(), anyMap());
    Assert.assertEquals(oldVersionCaptor.getValue(), version2.kafkaTopicName());
    Assert.assertEquals(newVersionCaptor.getValue(), version3.kafkaTopicName());
  }

  public void testEnsurePreconditions() {
    String srcTopic = "testTopic_rt";
    String destTopic = "testTopic_v1";
    Store mockStore = mock(Store.class);
    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);

    doReturn(true).when(mockStore).isHybrid();
    doReturn(mockHybridConfig).when(mockStore).getHybridStoreConfig();
    Version version = new VersionImpl(Version.parseStoreFromKafkaTopicName(destTopic), 1, "test-id");
    doReturn(Optional.of(version)).when(mockStore).getVersion(Version.parseVersionFromKafkaTopicName(destTopic));
    doReturn(3600L).when(mockHybridConfig).getRewindTimeInSeconds();
    doReturn(REWIND_FROM_EOP).when(mockHybridConfig).getBufferReplayPolicy();
    doReturn(false).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(srcTopic);
    doReturn(true).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(destTopic);

    leaderStorageNodeReplicator.ensurePreconditions(srcTopic, destTopic, mockStore, Optional.of(mockHybridConfig));

    verify(mockTopicManager).createTopic(
        eq(srcTopic),
        anyInt(),
        eq(KAFKA_RF_FOR_RT_TOPICS),
        anyLong(),
        eq(false),
        eq(Optional.of(KAFKA_MIN_ISR_FOR_RT_TOPICS)),
        eq(false));
  }
}
