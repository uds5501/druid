/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.kafka.simulate;

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorTuningConfig;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.LagBasedAutoScalerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.simulate.EmbeddedBroker;
import org.apache.druid.testing.simulate.EmbeddedCoordinator;
import org.apache.druid.testing.simulate.EmbeddedDruidCluster;
import org.apache.druid.testing.simulate.EmbeddedHistorical;
import org.apache.druid.testing.simulate.EmbeddedIndexer;
import org.apache.druid.testing.simulate.EmbeddedOverlord;
import org.apache.druid.testing.simulate.EmbeddedRouter;
import org.apache.druid.testing.simulate.emitter.LatchableEmitterModule;
import org.apache.druid.testing.simulate.junit5.IndexingSimulationTestBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class KafkaSupervisorSimTest extends IndexingSimulationTestBase
{
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
    indexer.addProperty("druid.worker.capacity", "10");

    cluster.addExtension(KafkaIndexTaskModule.class)
           .addExtension(LatchableEmitterModule.class)
           .addResource(kafkaServer)
           .useLatchableEmitter()
           .addServer(new EmbeddedCoordinator())
           .addServer(overlord)
           .addServer(indexer)
           .addServer(broker)
           .addServer(historical)
           .addServer(new EmbeddedRouter());

    return cluster;
  }

  @Test
  public void test_runKafkaSupervisor()
  {
    final String topic = dataSource;
    kafkaServer.createTopicWithPartitions(topic, 2);

    kafkaServer.produceRecordsToTopic(
        generateRecordsForTopic(topic, 10, DateTimes.of("2025-06-01"))
    );

    // Submit and start a supervisor
    final String supervisorId = dataSource + "_supe";
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor(supervisorId, topic);

    final Map<String, String> startSupervisorResult = getResult(
        cluster.leaderOverlord().postSupervisor(kafkaSupervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", supervisorId), startSupervisorResult);

    // Wait for the broker to discover the realtime segments
    broker.latchableEmitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    SupervisorStatus supervisorStatus = getSupervisorStatus(supervisorId);
    Assertions.assertFalse(supervisorStatus.isSuspended());
    Assertions.assertTrue(supervisorStatus.isHealthy());
    Assertions.assertEquals(dataSource, supervisorStatus.getDataSource());
    Assertions.assertEquals("RUNNING", supervisorStatus.getState());
    Assertions.assertEquals(topic, supervisorStatus.getSource());

    // Get the task ID
    List<TaskStatusPlus> taskStatuses = ImmutableList.copyOf(
        getResult(cluster.leaderOverlord().taskStatuses(null, dataSource, 1))
    );
    Assertions.assertEquals(1, taskStatuses.size());
    Assertions.assertEquals(TaskState.RUNNING, taskStatuses.get(0).getStatusCode());

    // Verify the count of rows ingested into the datasource so far
    Assertions.assertEquals("10", runSql("SELECT COUNT(*) FROM %s", dataSource));

    // Suspend the supervisor and verify the state
    getResult(
        cluster.leaderOverlord().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec())
    );
    supervisorStatus = getSupervisorStatus(supervisorId);
    Assertions.assertTrue(supervisorStatus.isSuspended());
  }

  @Test
  public void test_runKafkaSupervisor_withAutoScaler()
  {
    final String topic = dataSource;
    final int partitionCount = 15;
    final int recordsPerPartition = 5;
    final int taskCountMin = 14;

    kafkaServer.createTopicWithPartitions(topic, partitionCount);

    kafkaServer.produceRecordsToTopic(
        generateRecordsForTopicPerPartition(topic, recordsPerPartition, DateTimes.of("2025-06-01"), partitionCount)
    );

    // Submit and start a supervisor
    final String supervisorId = dataSource + "_supe";
    final AutoScalerConfig autoScalerConfig = new LagBasedAutoScalerConfig(
        null, null, null, null, null, null, null, null,
        partitionCount, null, taskCountMin, null, null, true, null, null
    );
    final KafkaSupervisorIOConfig supervisorIOConfig = new KafkaSupervisorIOConfig(
        topic,
        null,
        new CsvInputFormat(List.of("timestamp", "item"), null, null, false, 0, false),
        null, null,
        new Period("PT1M"),
        kafkaServer.consumerProperties(),
        autoScalerConfig, null, null, null, null,
        true,
        new Period("PT2M"), null, null, null, null, null, null, null
    );
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor(supervisorId, topic, supervisorIOConfig);

    final Map<String, String> startSupervisorResult = getResult(
        cluster.leaderOverlord().postSupervisor(kafkaSupervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", supervisorId), startSupervisorResult);

    // Wait for the broker to discover the realtime segments
    broker.latchableEmitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    SupervisorStatus supervisorStatus = getSupervisorStatus(supervisorId);
    Assertions.assertFalse(supervisorStatus.isSuspended());
    Assertions.assertTrue(supervisorStatus.isHealthy());
    Assertions.assertEquals(dataSource, supervisorStatus.getDataSource());
    Assertions.assertEquals("RUNNING", supervisorStatus.getState());
    Assertions.assertEquals(topic, supervisorStatus.getSource());


    // Get the task ID
    List<TaskStatusPlus> taskStatuses = ImmutableList.copyOf(
        getResult(cluster.leaderOverlord().taskStatuses(null, dataSource, partitionCount))
    );

    Assertions.assertTrue(taskCountMin <= taskStatuses.size());
    assertTaskIsRunningOrSuccessful(taskStatuses);

    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/run/time")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasCount(taskCountMin)
    );

    // Verify the count of rows ingested into the datasource so far
    final int numRows = Integer.parseInt(
        runSql("SELECT COUNT(*) FROM %s", dataSource)
    );
    Assertions.assertEquals(partitionCount * recordsPerPartition, numRows);
  }

  @Test
  public void test_runKafkaSupervisor_withIOConfigTaskCount()
  {
    final String topic = dataSource;
    final int partitionCount = 10;
    final int recordsPerPartition = 10;
    final int taskCount = 10;

    kafkaServer.createTopicWithPartitions(topic, partitionCount);

    kafkaServer.produceRecordsToTopic(
        generateRecordsForTopicPerPartition(topic, recordsPerPartition, DateTimes.of("2025-06-01"), partitionCount)
    );

    // Submit and start a supervisor
    final String supervisorId = dataSource + "_supe";
    final KafkaSupervisorIOConfig supervisorIOConfig = new KafkaSupervisorIOConfig(
        topic,
        null,
        new CsvInputFormat(List.of("timestamp", "item"), null, null, false, 0, false),
        null, taskCount,
        new Period("PT1M"),
        kafkaServer.consumerProperties(),
        null, null, null, null, null,
        true,
        new Period("PT2M"), null, null, null, null, null, null, null
    );
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor(supervisorId, topic, supervisorIOConfig);

    final Map<String, String> startSupervisorResult = getResult(
        cluster.leaderOverlord().postSupervisor(kafkaSupervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", supervisorId), startSupervisorResult);

    // Wait for the broker to discover the realtime segments
    broker.latchableEmitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    SupervisorStatus supervisorStatus = getSupervisorStatus(supervisorId);
    Assertions.assertFalse(supervisorStatus.isSuspended());
    Assertions.assertTrue(supervisorStatus.isHealthy());
    Assertions.assertEquals(dataSource, supervisorStatus.getDataSource());
    Assertions.assertEquals("RUNNING", supervisorStatus.getState());
    Assertions.assertEquals(topic, supervisorStatus.getSource());


    // Get the task ID
    List<TaskStatusPlus> taskStatuses = ImmutableList.copyOf(
        getResult(cluster.leaderOverlord().taskStatuses(null, dataSource, partitionCount))
    );

    Assertions.assertTrue(taskCount <= taskStatuses.size());
    assertTaskIsRunningOrSuccessful(taskStatuses);

    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/run/time")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasCount(taskCount)
    );

    // Verify the count of rows ingested into the datasource so far
    final int numRows = Integer.parseInt(
        runSql("SELECT COUNT(*) FROM %s", dataSource)
    );
    Assertions.assertEquals(partitionCount * recordsPerPartition, numRows);
  }

  private void assertTaskIsRunningOrSuccessful(List<TaskStatusPlus> taskStatuses)
  {
    for (TaskStatusPlus taskStatus : taskStatuses) {
      TaskState statusCode = taskStatus.getStatusCode();
      Assertions.assertTrue(
          TaskState.RUNNING.equals(statusCode) ||
          TaskState.SUCCESS.equals(statusCode),
          "Task should be either RUNNING or SUCCESS, but was: " + statusCode
      );
    }
  }

  private KafkaSupervisorSpec createKafkaSupervisor(String supervisorId, String topic)
  {
    return createKafkaSupervisor(
        supervisorId, topic, new KafkaSupervisorIOConfig(
            topic,
            null,
            new CsvInputFormat(List.of("timestamp", "item"), null, null, false, 0, false),
            null, null,
            null,
            kafkaServer.consumerProperties(),
            null, null, null, null, null,
            true,
            null, null, null, null, null, null, null, null
        )
    );
  }

  private KafkaSupervisorSpec createKafkaSupervisor(String supervisorId, String topic, KafkaSupervisorIOConfig ioConfig)
  {
    return new KafkaSupervisorSpec(
        supervisorId,
        null,
        DataSchema.builder()
                  .withDataSource(dataSource)
                  .withTimestamp(new TimestampSpec("timestamp", null, null))
                  .withDimensions(DimensionsSpec.EMPTY)
                  .build(),
        createTuningConfig(),
        ioConfig,
        null, null, null, null, null, null, null, null, null, null, null
    );
  }

  private KafkaSupervisorTuningConfig createTuningConfig()
  {
    return new KafkaSupervisorTuningConfig(
        null,
        null, null, null,
        1,
        null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null
    );
  }

  private List<ProducerRecord<byte[], byte[]>> generateRecordsForTopicPerPartition(
      String topic,
      int count,
      DateTime startTime,
      int partitionCount
  )
  {
    List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int partition = 0; partition < partitionCount; partition++) {
      records.addAll(generateRecordsForTopic(topic, count, startTime, partition));
    }
    return records;
  }

  private List<ProducerRecord<byte[], byte[]>> generateRecordsForTopic(
      String topic,
      int count,
      DateTime startTime
  )
  {
    return generateRecordsForTopic(topic, count, startTime, 0);
  }

  private List<ProducerRecord<byte[], byte[]>> generateRecordsForTopic(
      String topic,
      int count,
      DateTime startTime,
      int partition
  )
  {
    final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < count; ++i) {
      String valueCsv = StringUtils.format(
          "%s,%s,%d",
          startTime.plusDays(i),
          IdUtils.getRandomId(),
          ThreadLocalRandom.current().nextInt(1000)
      );
      records.add(
          new ProducerRecord<>(topic, partition, null, StringUtils.toUtf8(valueCsv))
      );
    }
    return records;
  }
}
