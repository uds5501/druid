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

import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.k8s.simulate.K3SResource;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for Kubernetes-based tests. Provides a K3s cluster
 * and manages K8s components lifecycle.
 */
public abstract class KubernetesTestBase
{
  private static final Logger log = new Logger(KubernetesTestBase.class);
  
  private final K3SResource k3sContainer = new K3SResource();
  private KubernetesClient client;
  private final List<K8sComponent> components = new ArrayList<>();

  protected void startK3SContainer()
  {
    log.info("Starting K3s conatiner...");
    k3sContainer.start();
    this.client = k3sContainer.getClient();
  }

  protected void stopK3SContainer()
  {
    log.info("Stopping K3s cluster...");
    k3sContainer.stop();
  }

  protected KubernetesClient getClient()
  {
    return client;
  }

  /**
   * Add a kubernetes component to be deployed in k3s container.
   *
   * @param component the component to add
   */
  protected void addKubernetesComponent(K8sComponent component)
  {
    components.add(component);
  }

  /**
   * Initialize all registered components in order.
   */
  protected void initializeComponents()
  {
    for (K8sComponent component : components) {
      try {
        component.initialize(client);
        component.waitUntilReady(client);
      } catch (Exception e) {
        log.error("Failed to initialize %s: %s", component.getComponentName(), e.getMessage());
        throw new RuntimeException("Component initialization failed", e);
      }
    }
  }

  protected void cleanupComponents()
  {
    for (int i = components.size() - 1; i >= 0; i--) {
      K8sComponent component = components.get(i);
      try {
        component.cleanup(client);
      } catch (Exception e) {
        log.error("Error cleaning up %s: %s", component.getComponentName(), e.getMessage());
      }
    }
    components.clear();
  }

  protected void createNamespace(String namespace)
  {
    try {
      client.namespaces().resource(new NamespaceBuilder()
          .withNewMetadata()
          .withName(namespace)
          .endMetadata()
          .build()).create();
    } catch (Exception e) {
      log.error("Error creating namespace %s: %s", namespace, e.getMessage());
      throw new RuntimeException("Failed to create namespace", e);
    }
  }

  /**
   * Get cluster state for debugging purposes.
   */
  protected void printClusterState()
  {
    log.info("=== Kubernetes Cluster State ===");

    try {
      printNamespaces();
      printAllPods();
      log.info("=== End Cluster State ===");
    } catch (Exception e) {
      log.error("Error retrieving cluster state: %s", e.getMessage());
    }
  }

  private void printNamespaces()
  {
    log.info("--- Namespaces ---");
    client.namespaces().list().getItems().forEach(ns ->
        log.info("- %s", ns.getMetadata().getName())
    );
  }

  private void printAllPods()
  {
    log.info("--- All Pods ---");
    client.pods().inAnyNamespace().list().getItems().forEach(pod ->
        log.info("- %s (namespace: %s, status: %s)", 
            pod.getMetadata().getName(),
            pod.getMetadata().getNamespace(),
            pod.getStatus().getPhase())
    );
  }
}