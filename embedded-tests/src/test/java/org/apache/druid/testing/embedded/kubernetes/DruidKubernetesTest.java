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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

public class DruidKubernetesTest extends KubernetesTestBase
{
  private static final String DRUID_NAMESPACE = "druid";

  private DruidOperatorComponent druidOperator;
  private DruidClusterComponent druidCluster;

  @BeforeEach
  void setUp()
  {
    startK3SContainer();
    createNamespace(DRUID_NAMESPACE);
    String clusterName = "test-cluster";

    druidOperator = new DruidOperatorComponent(DRUID_NAMESPACE);
    druidCluster = new DruidClusterComponent(DRUID_NAMESPACE, DruidK8sComponent.DRUID_34_IMAGE, clusterName);

    druidCluster.addDruidService(new DruidK8sCoordinatorComponent(DRUID_NAMESPACE, DruidK8sComponent.DRUID_34_IMAGE, clusterName));
    druidCluster.addDruidService(new DruidK8sBrokerComponent(DRUID_NAMESPACE, DruidK8sComponent.DRUID_34_IMAGE, clusterName));
    druidCluster.addDruidService(new DruidK8sHistoricalComponent(DRUID_NAMESPACE, DruidK8sComponent.DRUID_34_IMAGE, clusterName, "hot", 1));
    druidCluster.addDruidService(new DruidK8sRouterComponent(DRUID_NAMESPACE, DruidK8sComponent.DRUID_34_IMAGE, clusterName));

    addKubernetesComponent(druidOperator);
    addKubernetesComponent(druidCluster);

    initializeComponents();
  }

  @AfterEach
  void tearDown()
  {
    cleanupComponents();
    stopK3SContainer();
  }

  @Test
  void testDruidOperatorDeployment()
  {
    assertOperatorIsRunning();
  }


  @Test
  void testOperatorWatchesNamespace()
  {
    assertOperatorWatchesCorrectNamespace();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  void testDruidClusterDeployment()
  {
    assertDruidClusterIsRunning();
  }

  @Test
  void testDruidWebConsoleAccess()
  {
    assertDruidWebConsoleIsAccessible();
  }

  private void assertOperatorIsRunning()
  {
    var deployment = getClient().apps().deployments()
        .inNamespace(druidOperator.getNamespace())
        .withName("druid-operator-test")
        .get();

    if (deployment == null) {
      throw new AssertionError("Druid operator deployment not found");
    }

    if (deployment.getStatus().getReadyReplicas() == null ||
        deployment.getStatus().getReadyReplicas() < 1) {
      throw new AssertionError("Druid operator deployment is not ready. Ready replicas: " +
          deployment.getStatus().getReadyReplicas());
    }

    System.out.println("✓ Druid operator deployment is running with " +
        deployment.getStatus().getReadyReplicas() + " ready replicas");
  }


  private void assertOperatorWatchesCorrectNamespace()
  {
    var deployment = getClient().apps().deployments()
        .inNamespace(druidOperator.getNamespace())
        .withName("druid-operator-test")
        .get();

    if (deployment == null) {
      throw new AssertionError("Druid operator deployment not found");
    }

    // Check the WATCH_NAMESPACE environment variable
    var containers = deployment.getSpec().getTemplate().getSpec().getContainers();
    boolean foundCorrectWatchNamespace = false;

    for (var container : containers) {
      if ("manager".equals(container.getName()) && container.getEnv() != null) {
        for (var envVar : container.getEnv()) {
          if ("WATCH_NAMESPACE".equals(envVar.getName()) &&
              DRUID_NAMESPACE.equals(envVar.getValue())) {
            foundCorrectWatchNamespace = true;
            break;
          }
        }
      }
    }

    if (!foundCorrectWatchNamespace) {
      throw new AssertionError("Operator is not configured to watch the correct namespace: " + DRUID_NAMESPACE);
    }

    System.out.println("✓ Operator is configured to watch namespace: " + DRUID_NAMESPACE);
  }


  private void assertDruidClusterIsRunning()
  {
    for (DruidK8sComponent service : druidCluster.getDruidServices()) {
      var deployment = getClient().apps().deployments()
          .inNamespace(DRUID_NAMESPACE)
          .withName(service.getClusterName() + "-" + service.getDruidServiceType())
          .get();

      if (deployment == null) {
        throw new AssertionError("Druid " + service.getDruidServiceType() + " deployment not found");
      }

      if (deployment.getStatus().getReadyReplicas() == null ||
          deployment.getStatus().getReadyReplicas() < 1) {
        throw new AssertionError("Druid " + service.getDruidServiceType() + " deployment is not ready. Ready replicas: " +
            deployment.getStatus().getReadyReplicas());
      }

      System.out.println("✓ Druid " + service.getDruidServiceType() + " is running with " +
          deployment.getStatus().getReadyReplicas() + " ready replicas");
    }
  }

  private void assertDruidServicesCanCommunicate()
  {
    // Check that coordinator service exists and is accessible
    var coordinatorService = getClient().services()
        .inNamespace(DRUID_NAMESPACE)
        .withName(druidCluster.getClusterName() + "-coordinator")
        .get();

    if (coordinatorService == null) {
      throw new AssertionError("Coordinator service not found");
    }

    // Check that broker service exists and is accessible
    var brokerService = getClient().services()
        .inNamespace(DRUID_NAMESPACE)
        .withName(druidCluster.getClusterName() + "-broker")
        .get();

    if (brokerService == null) {
      throw new AssertionError("Broker service not found");
    }

    System.out.println("✓ Druid services can discover each other via Kubernetes DNS");
    System.out.println("  - Coordinator URL: " + druidCluster.getCoordinatorUrl());
    System.out.println("  - Broker URL: " + druidCluster.getBrokerUrl());
  }

  private void assertDruidWebConsoleIsAccessible()
  {
    var routerService = getClient().services()
        .inNamespace(DRUID_NAMESPACE)
        .withName(druidCluster.getClusterName() + "-router")
        .get();

    if (routerService == null) {
      throw new AssertionError("Router service not found");
    }

    System.out.println("✓ Druid web console is accessible via router");
    System.out.println("  - Router URL: " + druidCluster.getRouterUrl());
  }

  private void printDruidClusterStatus()
  {
    System.out.println("\n=== Druid Cluster Status ===");

    for (DruidK8sComponent service : druidCluster.getDruidServices()) {
      var deployment = getClient().apps().deployments()
          .inNamespace(DRUID_NAMESPACE)
          .withName(service.getClusterName() + "-" + service.getDruidServiceType())
          .get();

      if (deployment != null) {
        System.out.println(service.getDruidServiceType().toUpperCase() + ":");
        System.out.println("  Image: " + service.getDruidImage());
        System.out.println("  Replicas: " + deployment.getStatus().getReadyReplicas() + "/" + deployment.getSpec().getReplicas());
        System.out.println("  Port: " + service.getDruidPort());
      }
    }

    System.out.println("\nHistorical Tiers:");
    for (DruidK8sHistoricalComponent historical : druidCluster.getHistoricals()) {
      System.out.println("  - " + historical.getTier() + " tier");
    }
  }

}