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

import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.indexing.Resources;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Component that orchestrates a complete Druid cluster deployment.
 * Manages all Druid services and their dependencies.
 */
public class DruidClusterComponent implements K8sComponent
{
  private static final Logger log = new Logger(DruidClusterComponent.class);

  private static final String COMPONENT_NAME = "DruidCluster";
  private static final String RBAC_MANIFEST_PATH = "data/manifests/druid-common/rbac.yaml";

  private final String namespace;
  private final String druidImage;
  private final String clusterName;
  private final List<DruidK8sComponent> druidServices = new ArrayList<>();

  public DruidClusterComponent(String namespace, String druidImage, String clusterName)
  {
    this.namespace = namespace;
    this.druidImage = druidImage;
    this.clusterName = clusterName;
  }


  public void addDruidService(DruidK8sComponent service)
  {
    druidServices.add(service);
  }

  @Override
  public void initialize(KubernetesClient client) throws Exception
  {
    log.info("Initializing %s...", getComponentName());
    
    // Apply RBAC manifests first
    applyRBACManifests(client);
    
    // Apply single Druid manifest with all nodes
    applyDruidClusterManifest(client);
    
    log.info("%s initialization completed", getComponentName());
  }

  @Override
  public void waitUntilReady(KubernetesClient client) throws Exception
  {
    log.info("Waiting for %s to be ready...", getComponentName());
    
    for (DruidK8sComponent service : druidServices) {
      log.info("Waiting for Druid %s to be ready...", service.getDruidServiceType());
      service.waitUntilReady(client);
    }
    
    log.info("%s is ready", getComponentName());
  }

  @Override
  public void cleanup(KubernetesClient client)
  {
    log.info("Cleaning up %s...", getComponentName());
    
    // Clean up individual Druid custom resources for each component
    for (DruidK8sComponent service : druidServices) {
      try {
        String componentName = "druid-" + service.getDruidServiceType();
        client.genericKubernetesResources("druid.apache.org/v1alpha1", "Druid")
            .inNamespace(namespace)
            .withName(componentName)
            .delete();
        log.info("Deleted Druid custom resource for %s", service.getDruidServiceType());
      } catch (Exception e) {
        log.error("Error deleting Druid custom resource for %s: %s", service.getDruidServiceType(), e.getMessage());
      }
    }
    
    // Clean up RBAC resources
    cleanupRBACResources(client);
    
    log.info("%s cleanup completed", getComponentName());
  }

  @Override
  public String getComponentName()
  {
    return COMPONENT_NAME;
  }

  @Override
  public String getNamespace()
  {
    return namespace;
  }

  public String getDruidImage()
  {
    return druidImage;
  }

  public String getClusterName()
  {
    return clusterName;
  }

  public List<DruidK8sComponent> getDruidServices()
  {
    return new ArrayList<>(druidServices);
  }

  public DruidK8sComponent getCoordinator()
  {
    return druidServices.stream()
        .filter(service -> "coordinator".equals(service.getDruidServiceType()))
        .findFirst()
        .orElse(null);
  }

  public DruidK8sComponent getBroker()
  {
    return druidServices.stream()
        .filter(service -> "broker".equals(service.getDruidServiceType()))
        .findFirst()
        .orElse(null);
  }

  public DruidK8sComponent getRouter()
  {
    return druidServices.stream()
        .filter(service -> "router".equals(service.getDruidServiceType()))
        .findFirst()
        .orElse(null);
  }

  public List<DruidK8sHistoricalComponent> getHistoricals()
  {
    return druidServices.stream()
        .filter(service -> "historical".equals(service.getDruidServiceType()))
        .map(service -> (DruidK8sHistoricalComponent) service)
        .collect(java.util.stream.Collectors.toList());
  }

  public String getCoordinatorUrl()
  {
    DruidK8sComponent coordinator = getCoordinator();
    if (coordinator != null) {
      return String.format("http://%s.%s.svc.cluster.local:%d", 
          coordinator.getClusterName() + "-coordinator", 
          namespace, 
          coordinator.getDruidPort());
    }
    return null;
  }

  public String getBrokerUrl()
  {
    DruidK8sComponent broker = getBroker();
    if (broker != null) {
      return String.format("http://%s.%s.svc.cluster.local:%d", 
          broker.getClusterName() + "-broker", 
          namespace, 
          broker.getDruidPort());
    }
    return null;
  }

  public String getRouterUrl()
  {
    DruidK8sComponent router = getRouter();
    if (router != null) {
      return String.format("http://%s.%s.svc.cluster.local:%d", 
          router.getClusterName() + "-router", 
          namespace, 
          router.getDruidPort());
    }
    return null;
  }

  private void applyRBACManifests(KubernetesClient client)
  {
    log.info("Applying Druid RBAC manifest...");

    try {
      client.load(new FileInputStream(Resources.getFileForResource(RBAC_MANIFEST_PATH)))
          .inNamespace(namespace)
          .createOrReplace();
      log.info("Applied RBAC manifest: %s", RBAC_MANIFEST_PATH);
    } catch (Exception e) {
      log.error("Error applying RBAC manifest %s: %s", RBAC_MANIFEST_PATH, e.getMessage());
      throw new RuntimeException("Failed to apply RBAC manifest: " + RBAC_MANIFEST_PATH, e);
    }
  }

  private void cleanupRBACResources(KubernetesClient client)
  {
    try {
      log.info("Cleaning up Druid RBAC resources...");
      
      client.rbac().roles().inNamespace(namespace).withName("druid-cluster").delete();
      client.rbac().roleBindings().inNamespace(namespace).withName("druid-cluster").delete();
      
      log.info("Druid RBAC cleanup completed");
    } catch (Exception e) {
      log.error("Error cleaning up RBAC resources: %s", e.getMessage());
    }
  }

  public void printClusterInfo()
  {
    log.info("=== DRUID CLUSTER INFO ===");
    log.info("Cluster Name: %s", clusterName);
    log.info("Namespace: %s", namespace);
    log.info("Druid Image: %s", druidImage);
    log.info("Services:");
    
    for (DruidK8sComponent service : druidServices) {
      log.info("  - %s: %s:%d", 
          service.getDruidServiceType(), 
          service.getClusterName() + "-" + service.getDruidServiceType(), 
          service.getDruidPort());
    }
    
    log.info("URLs:");
    log.info("  - Coordinator: %s", getCoordinatorUrl());
    log.info("  - Broker: %s", getBrokerUrl());
    log.info("  - Router (Web Console): %s", getRouterUrl());
    log.info("========================");
  }
  
  /**
   * Apply individual Druid component manifests.
   */
  private void applyDruidClusterManifest(KubernetesClient client) throws Exception
  {
    log.info("Applying individual Druid component manifests...");
    
    // Apply each component's manifest in standalone mode
    for (DruidK8sComponent service : druidServices) {
      log.info("Applying manifest for Druid %s...", service.getDruidServiceType());
      service.applyDruidManifest(client);
    }
    
    log.info("Applied all Druid component manifests");
  }
  
}