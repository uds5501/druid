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

package org.apache.druid.testing.embedded.kubernetes;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSetList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DruidKubernetesTest extends KubernetesTestBase
{
  private static final String DRUID_NAMESPACE = "druid";

  private static DruidOperatorComponent druidOperator;
  private DruidClusterComponent druidCluster;

  @BeforeAll
  public static void init()
  {
    startK3SContainer();
    createNamespace(DRUID_NAMESPACE);
    druidOperator = new DruidOperatorComponent(DRUID_NAMESPACE);
    addKubernetesComponent(druidOperator, false);
    initializeComponents();
  }

  @BeforeEach
  public void setUp()
  {
    String clusterName = "test-cluster";
    druidCluster = new DruidClusterComponent(DRUID_NAMESPACE, DruidK8sComponent.DRUID_34_IMAGE, clusterName);

    druidCluster.addDruidService(new DruidK8sCoordinatorComponent(
        DRUID_NAMESPACE,
        DruidK8sComponent.DRUID_34_IMAGE,
        clusterName
    ));
    druidCluster.addDruidService(new DruidK8sBrokerComponent(
        DRUID_NAMESPACE,
        DruidK8sComponent.DRUID_34_IMAGE,
        clusterName
    ));
    druidCluster.addDruidService(new DruidK8sHistoricalComponent(
        DRUID_NAMESPACE,
        DruidK8sComponent.DRUID_34_IMAGE,
        clusterName,
        "hot",
        1
    ));
    druidCluster.addDruidService(new DruidK8sRouterComponent(
        DRUID_NAMESPACE,
        DruidK8sComponent.DRUID_34_IMAGE,
        clusterName
    ));
    addKubernetesComponent(druidCluster);
    initializeComponents();
  }

  @AfterEach
  void tearDown()
  {
    cleanupComponents(false);

    try {
      Thread.sleep(5000); // 5 seconds
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @AfterAll
  public static void cleanup()
  {
    cleanupComponents(true);
    stopK3SContainer();
  }

  @Test
  public void test_operator_deployment()
  {
    Deployment deployment = getClient().apps().deployments()
                                       .inNamespace(druidOperator.getNamespace())
                                       .withName("druid-operator-test")
                                       .get();

    if (deployment == null) {
      throw new AssertionError("Druid operator deployment not found");
    }

    Assertions.assertNotNull(deployment.getStatus().getReadyReplicas());
    Assertions.assertTrue(
        deployment.getStatus().getReadyReplicas() >= 1,
        "Druid operator deployment should have at least 1 ready replica"
    );
  }

  @Test
  public void test_operator_namespace_watching()
  {
    Deployment deployment = getClient().apps().deployments()
                                       .inNamespace(druidOperator.getNamespace())
                                       .withName("druid-operator-test")
                                       .get();

    if (deployment == null) {
      throw new AssertionError("Druid operator deployment not found");
    }

    List<Container> containers = deployment.getSpec().getTemplate().getSpec().getContainers();
    containers.stream()
              .filter(container -> "manager".equals(container.getName()) && container.getEnv() != null)
              .flatMap(container -> container.getEnv().stream())
              .filter(envVar -> "WATCH_NAMESPACE".equals(envVar.getName()))
              .findFirst()
              .ifPresent(envVar -> Assertions.assertEquals(DRUID_NAMESPACE, envVar.getValue()));
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES)
  public void test_cluster_deployment()
  {
    for (DruidK8sComponent service : druidCluster.getDruidServices()) {
      String uniqueLabel = service.getPodLabel();
      AtomicBoolean found = new AtomicBoolean(false);
      StatefulSetList statefulSetsByLabel = getClient().apps().statefulSets()
                                                       .inNamespace(DRUID_NAMESPACE)
                                                       .withLabel("nodeSpecUniqueStr", uniqueLabel)
                                                       .list();

      statefulSetsByLabel.getItems().stream()
                         .findFirst()
                         .ifPresent(statefulSet -> {
                           found.set(true);
                           Assertions.assertNotNull(
                               statefulSet.getStatus().getReadyReplicas(),
                               "ReadyReplicas should not be null for " + service.getDruidServiceType()
                           );
                           Assertions.assertTrue(
                               statefulSet.getStatus().getReadyReplicas() >= 1,
                               "Druid " + service.getDruidServiceType() + " statefulset is not ready. Ready replicas: "
                               + statefulSet.getStatus().getReadyReplicas()
                           );
                         });
      if (!found.get()) {
        throw new AssertionError("Druid "
                                 + service.getDruidServiceType()
                                 + " statefulset not found by nodeSpecUniqueStr label: "
                                 + uniqueLabel);
      }
    }
  }
}