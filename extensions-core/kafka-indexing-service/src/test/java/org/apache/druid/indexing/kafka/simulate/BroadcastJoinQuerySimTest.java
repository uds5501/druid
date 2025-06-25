package org.apache.druid.indexing.kafka.simulate;

import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.server.coordinator.rules.ForeverBroadcastDistributionRule;
import org.apache.druid.testing.simulate.EmbeddedBroker;
import org.apache.druid.testing.simulate.EmbeddedCoordinator;
import org.apache.druid.testing.simulate.EmbeddedDruidCluster;
import org.apache.druid.testing.simulate.EmbeddedHistorical;
import org.apache.druid.testing.simulate.EmbeddedIndexer;
import org.apache.druid.testing.simulate.EmbeddedOverlord;
import org.apache.druid.testing.simulate.junit5.IndexingSimulationTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class BroadcastJoinQuerySimTest extends IndexingSimulationTestBase
{
  private static final String BROADCAST_JOIN_TASK = "/indexer/broadcast_join_index_task.json";
  private static final String BROADCAST_JOIN_METADATA_QUERIES_RESOURCE = "/queries/broadcast_join_metadata_queries.json";
  private static final String BROADCAST_JOIN_METADATA_QUERIES_AFTER_DROP_RESOURCE = "/queries/broadcast_join_after_drop_metadata_queries.json";
  private static final String BROADCAST_JOIN_QUERIES_RESOURCE = "/queries/broadcast_join_queries.json";
  private static final String BROADCAST_JOIN_DATASOURCE = "broadcast_join_wikipedia_test";

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private EmbeddedKafkaServer kafkaServer;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();

    kafkaServer = new EmbeddedKafkaServer(cluster.getZookeeper(), cluster.getTestFolder(), Map.of());

    cluster.addExtension(KafkaIndexTaskModule.class)
           .addResource(kafkaServer)
           .useLatchableEmitter()
           .addServer(new EmbeddedCoordinator())
           .addServer(overlord)
           .addServer(indexer)
           .addServer(historical)
           .addServer(broker);

    return cluster;
  }

  @BeforeEach
  public void setUp() {
    insertSegments();
  }

  @Test
  public void test_broadcastJoinQuery_centralizedDatasource()
  {
    cluster.leaderCoordinator().postLoadRules(BROADCAST_JOIN_DATASOURCE, ImmutableList.of(new ForeverBroadcastDistributionRule()));
  }

  private void insertSegments()
  {
    final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = cluster.getInMemoryDerbyResource().getDbRule();
    final TestDerbyConnector.SegmentsTable segments = derbyConnectorRule.segments();

    segments.insert(
        "INSERT INTO %1$s (id, dataSource, version, interval, partitionNum, shardSpec, binaryVersion, size, loadSpec) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        "1",
        dataSource,
        "2023-10-01T00:00:00.000Z",
        "2023-10-01T00:00:00.000Z/2023-10-02T00:00:00.000Z",
        0,
        "single",
        9,
        1000L,
        "{\"type\":\"local\",\"path\":\"/tmp/wikipedia_data\"}"
    );
    // For twitterstream segment 1
    segments.insert(
        "INSERT INTO %1$s (id, dataSource, version, interval, partitionNum, shardSpec, binaryVersion, size, loadSpec) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        "twitterstream_2013-01-02T00:00:00.000Z_2013-01-03T00:00:00.000Z_2013-01-03T03:44:58.791Z_v9",
        "twitterstream",
        "2013-01-03T03:44:58.791Z_v9",
        "2013-01-02T00:00:00.000Z/2013-01-03T00:00:00.000Z",
        0,
        "{\"type\":\"none\"}",
        9,
        435325540L,
        "{\"type\":\"s3_zip\",\"bucket\":\"static.druid.io\",\"key\":\"data/segments/twitterstream/2013-01-02T00:00:00.000Z_2013-01-03T00:00:00.000Z/2013-01-03T03:44:58.791Z_v9/0/index.zip\"}"
    );

    // For twitterstream segment 2
    segments.insert(
        "INSERT INTO %1$s (id, dataSource, version, interval, partitionNum, shardSpec, binaryVersion, size, loadSpec) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        "twitterstream_2013-01-03T00:00:00.000Z_2013-01-04T00:00:00.000Z_2013-01-04T04:09:13.590Z_v9",
        "twitterstream",
        "2013-01-04T04:09:13.590Z_v9",
        "2013-01-03T00:00:00.000Z/2013-01-04T00:00:00.000Z",
        0,
        "{\"type\":\"none\"}",
        9,
        411651320L,
        "{\"type\":\"s3_zip\",\"bucket\":\"static.druid.io\",\"key\":\"data/segments/twitterstream/2013-01-03T00:00:00.000Z_2013-01-04T00:00:00.000Z/2013-01-04T04:09:13.590Z_v9/0/index.zip\"}"
    );

    // For wikipedia_editstream segment
    segments.insert(
        "INSERT INTO %1$s (id, dataSource, version, interval, partitionNum, shardSpec, binaryVersion, size, loadSpec) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        "wikipedia_editstream_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9",
        "wikipedia_editstream",
        "2013-01-10T08:13:47.830Z_v9",
        "2012-12-29T00:00:00.000Z/2013-01-10T08:00:00.000Z",
        0,
        "{\"type\":\"none\"}",
        9,
        446027801L,
        "{\"type\":\"s3_zip\",\"bucket\":\"static.druid.io\",\"key\":\"data/segments/wikipedia_editstream/2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z/2013-01-10T08:13:47.830Z_v9/0/index.zip\"}"
    );

    // For wikipedia segment
    segments.insert(
        "INSERT INTO %1$s (id, dataSource, version, interval, partitionNum, shardSpec, binaryVersion, size, loadSpec) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        "wikipedia_2013-08-01T00:00:00.000Z_2013-08-02T00:00:00.000Z_2013-08-08T21:22:48.989Z",
        "wikipedia",
        "2013-08-08T21:22:48.989Z",
        "2013-08-01T00:00:00.000Z/2013-08-02T00:00:00.000Z",
        0,
        "{\"type\":\"none\"}",
        9,
        24664730L,
        "{\"type\":\"s3_zip\",\"bucket\":\"static.druid.io\",\"key\":\"data/segments/wikipedia/20130801T000000.000Z_20130802T000000.000Z/2013-08-08T21_22_48.989Z/0/index.zip\"}"
    );
  }
}
