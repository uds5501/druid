package org.apache.druid.indexing.kafka.simulate;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.coordinator.rules.ForeverBroadcastDistributionRule;
import org.apache.druid.testing.simulate.EmbeddedBroker;
import org.apache.druid.testing.simulate.EmbeddedCoordinator;
import org.apache.druid.testing.simulate.EmbeddedDruidCluster;
import org.apache.druid.testing.simulate.EmbeddedHistorical;
import org.apache.druid.testing.simulate.EmbeddedIndexer;
import org.apache.druid.testing.simulate.EmbeddedOverlord;
import org.apache.druid.testing.simulate.junit5.IndexingSimulationTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class BroadcastJoinQuerySimTest extends IndexingSimulationTestBase
{
  private static final String BROADCAST_JOIN_TASK = "/indexer/broadcast_join_index_task.json";
  private static final String BROADCAST_JOIN_METADATA_QUERIES_RESOURCE = "/queries/broadcast_join_metadata_queries.json";
  private static final String BROADCAST_JOIN_METADATA_QUERIES_AFTER_DROP_RESOURCE = "/queries/broadcast_join_after_drop_metadata_queries.json";
  private static final String BROADCAST_JOIN_QUERIES_RESOURCE = "/queries/broadcast_join_queries.json";
  private static final String BROADCAST_JOIN_DATASOURCE = "broadcast_join_wikipedia_test";
  private static final String WIKIPEDIA_DATA_SOURCE = "wikipedia_editstream";

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private EmbeddedKafkaServer kafkaServer;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();

//    kafkaServer = new EmbeddedKafkaServer(cluster.getZookeeper(), cluster.getTestFolder(), Map.of());

    cluster.addExtension(KafkaIndexTaskModule.class)
//           .addResource(kafkaServer)
           .useLatchableEmitter()
           .addServer(coordinator)
           .addServer(overlord)
           .addServer(indexer)
           .addServer(historical)
           .addServer(broker);

    return cluster;
  }

  @BeforeEach
  public void setUp()
  {
    insertSegments();
  }

  @Test
  public void test_broadcastJoinQuery_centralizedDatasource() throws IOException
  {
    cluster.leaderCoordinator()
           .postLoadRules(BROADCAST_JOIN_DATASOURCE, ImmutableList.of(new ForeverBroadcastDistributionRule()));
    String taskJson = replaceJoinTemplate(getResourceAsString(BROADCAST_JOIN_TASK), BROADCAST_JOIN_DATASOURCE);
    cluster.leaderOverlord().submitIndexTask(taskJson);

    // waiting for the indexing task to complete.
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/run/time")
                      .hasDimension(DruidMetrics.DATASOURCE, BROADCAST_JOIN_DATASOURCE),
        agg -> agg.hasCount(1)
    );

    // waiting for the coordinator to discover the segments
    coordinator.latchableEmitter().waitForEventAggregate(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, BROADCAST_JOIN_DATASOURCE)
                      .hasMetricName("segment/moved/count"),
        agg -> agg.hasCount(1)
    );

    Assertions.assertEquals(
        "1",
        runSql(
            replaceJoinTemplate(
                "SELECT \"%%JOIN_DATASOURCE%%\".\"user\", SUM(\"%%JOIN_DATASOURCE%%\".\"added\") FROM druid.\"%%JOIN_DATASOURCE%%\" GROUP BY 1 ORDER BY 2",
                BROADCAST_JOIN_DATASOURCE
            )
        )
    );
  }

  private void insertSegments()
  {

    final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = cluster.getInMemoryDerbyResource().getDbRule();
    derbyConnectorRule.getConnector().createSegmentTable();
    final TestDerbyConnector.SegmentsTable segments = derbyConnectorRule.segments();


    // Wikipedia segment
    String payloadString = "\"{\"dataSource\":\"wikipedia_editstream\",\"interval\":\"2012-12-29T00:00:00.000Z/2013-01-10T08:00:00.000Z\",\"version\":\"2013-01-10T08:13:47.830Z_v9\",\"loadSpec\":{\"type\":\"s3_zip\",\"bucket\":\"static.druid.io\",\"key\":\"data/segments/wikipedia_editstream/2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z/2013-01-10T08:13:47.830Z_v9/0/index.zip\"},\"dimensions\":\"anonymous,area_code,city,continent_code,country_name,dma_code,geo,language,namespace,network,newpage,page,postal_code,region_lookup,robot,unpatrolled,user\",\"metrics\":\"added,count,deleted,delta,delta_hist,unique_users,variation\",\"shardSpec\":{\"type\":\"none\"},\"binaryVersion\":9,\"size\":446027801,\"identifier\":\"wikipedia_editstream_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9\"}\"";
    byte[] payloadBytes = payloadString.getBytes(StandardCharsets.UTF_8);

    segments.insert(
        "INSERT INTO %1$s (id, dataSource, created_date, start, \\\"end\\\", partitioned, version, used, payload, used_status_last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        "wikipedia_editstream_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9",
        "wikipedia_editstream",
        "2013-03-15T20:49:52.348Z",
        "2012-12-29T00:00:00.000Z",
        "2013-01-10T08:00:00.000Z",
        0,
        "2013-01-10T08:13:47.830Z_v9",
        1,
        payloadBytes,
        "1970-01-01T00:00:00.000Z"
    );
  }

  private static String replaceJoinTemplate(String template, String joinDataSource)
  {
    return StringUtils.replace(
        StringUtils.replace(template, "%%JOIN_DATASOURCE%%", joinDataSource),
        "%%REGULAR_DATASOURCE%%",
        WIKIPEDIA_DATA_SOURCE
    );
  }

  private static String getResourceAsString(String file) throws IOException
  {
    try (final InputStream inputStream = getResourceAsStream(file)) {
      if (inputStream == null) {
        throw new ISE("Failed to load resource: [%s]", file);
      }
      return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }
  }

  public static InputStream getResourceAsStream(String resource)
  {
    return BroadcastJoinQuerySimTest.class.getResourceAsStream(resource);
  }
}
