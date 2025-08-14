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
import org.apache.druid.testing.embedded.indexing.Resources;

import java.io.FileInputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Component for deploying and managing the DataInfraHQ Druid Operator
 * in a Kubernetes test environment.
 */
public class DruidOperatorComponent implements K8sComponent
{
  private static final Logger log = new Logger(DruidOperatorComponent.class);
  
  private static final String COMPONENT_NAME = "DruidOperator";
  private static final String OPERATOR_NAMESPACE = "druid-operator-system";
  private static final String DEPLOYMENT_NAME = "druid-operator-test";
  private static final int POD_READY_CHECK_TIMEOUT_SECONDS = 120;

  private final String druidNamespace;

  private final List<String> manifestPaths = List.of(
      "data/manifests/druid-operator/crds/druid.apache.org_druids.yaml",
      "data/manifests/druid-operator/crds/druid.apache.org_druidingestions.yaml",
      "data/manifests/druid-operator/templates/service_account.yaml",
      "data/manifests/druid-operator/templates/rbac_manager.yaml",
      "data/manifests/druid-operator/templates/rbac_metrics.yaml",
      "data/manifests/druid-operator/templates/rbac_proxy.yaml",
      "data/manifests/druid-operator/templates/rbac_leader_election.yaml",
      "data/manifests/druid-operator/templates/deployment.yaml",
      "data/manifests/druid-operator/templates/service.yaml"
  );


  public DruidOperatorComponent(String druidNamespace) {this.druidNamespace = druidNamespace;}

  @Override
  public void initialize(KubernetesClient client) throws Exception
  {
    createNamespace(client, OPERATOR_NAMESPACE);
    createNamespace(client, druidNamespace);
    
    for (String manifestPath : manifestPaths) {
      try {
        client.load(new FileInputStream(Resources.getFileForResource(manifestPath))).create();
        log.info("Applied manifest: %s", manifestPath);
      } catch (Exception e) {
        log.error("Error applying manifest: %s", manifestPath);
        throw e;
      }
    }
  }

  @Override
  public void waitUntilReady(KubernetesClient client) throws Exception
  {
    try {
      client.apps().deployments()
          .inNamespace(OPERATOR_NAMESPACE)
          .withName(DEPLOYMENT_NAME)
          .waitUntilReady(POD_READY_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (Exception e) {
      log.error("Timeout waiting for %s to be ready", getComponentName());
      printDiagnostics(client);
      throw e;
    }
  }

  @Override
  public void cleanup(KubernetesClient client)
  {
    try {
      client.apps().deployments().inNamespace(OPERATOR_NAMESPACE).withName(DEPLOYMENT_NAME).delete();
      client.namespaces().withName(OPERATOR_NAMESPACE).delete();
    } catch (Exception e) {
      log.error("Error during %s cleanup: %s", getComponentName(), e.getMessage());
    }
  }

  @Override
  public String getComponentName()
  {
    return COMPONENT_NAME;
  }

  @Override
  public String getNamespace()
  {
    return OPERATOR_NAMESPACE;
  }

  private void createNamespace(KubernetesClient client, String namespace)
  {
    try {
      client.namespaces().resource(new NamespaceBuilder()
          .withNewMetadata()
          .withName(namespace)
          .endMetadata()
          .build()).createOrReplace();
    } catch (Exception e) {
      log.error("Error creating namespace %s: %s", namespace, e.getMessage());
    }
  }

  private void printDiagnostics(KubernetesClient client)
  {
    try {
      log.info("=== %s DIAGNOSTICS ===", getComponentName().toUpperCase());
      
      log.info("--- Pod Status ---");
      client.pods().inNamespace(OPERATOR_NAMESPACE).list().getItems().forEach(pod -> {
        log.info("Pod: %s", pod.getMetadata().getName());
        log.info("  Status: %s", pod.getStatus().getPhase());
        log.info("  Ready: %s", (pod.getStatus().getConditions() != null ? 
            pod.getStatus().getConditions().stream()
                .filter(c -> "Ready".equals(c.getType()))
                .findFirst()
                .map(c -> c.getStatus())
                .orElse("Unknown") : "Unknown"));
        
        // Container statuses
        if (pod.getStatus().getContainerStatuses() != null) {
          pod.getStatus().getContainerStatuses().forEach(containerStatus -> {
            log.info("  Container: " + containerStatus.getName());
            log.info("    Ready: " + containerStatus.getReady());
            log.info("    Restart Count: " + containerStatus.getRestartCount());
            
            if (containerStatus.getState() != null) {
              if (containerStatus.getState().getWaiting() != null) {
                log.info("    State: Waiting - " + containerStatus.getState().getWaiting().getReason());
                if (containerStatus.getState().getWaiting().getMessage() != null) {
                  log.info("    Message: " + containerStatus.getState().getWaiting().getMessage());
                }
              } else if (containerStatus.getState().getTerminated() != null) {
                log.info("    State: Terminated - " + containerStatus.getState().getTerminated().getReason());
                if (containerStatus.getState().getTerminated().getMessage() != null) {
                  log.info("    Message: " + containerStatus.getState().getTerminated().getMessage());
                }
                log.info("    Exit Code: " + containerStatus.getState().getTerminated().getExitCode());
              } else if (containerStatus.getState().getRunning() != null) {
                log.info("    State: Running since " + containerStatus.getState().getRunning().getStartedAt());
              }
            }
          });
        }
      });
      
      // Deployment status
      log.info("--- Deployment Status ---");
      var deployment = client.apps().deployments().inNamespace(OPERATOR_NAMESPACE).withName(DEPLOYMENT_NAME).get();
      if (deployment != null) {
        log.info("Deployment: " + deployment.getMetadata().getName());
        log.info("  Replicas: " + deployment.getSpec().getReplicas());
        log.info("  Ready Replicas: " + deployment.getStatus().getReadyReplicas());
        log.info("  Available Replicas: " + deployment.getStatus().getAvailableReplicas());
        
        if (deployment.getStatus().getConditions() != null) {
          deployment.getStatus().getConditions().forEach(condition -> {
            log.info("  Condition: " + condition.getType() + " = " + condition.getStatus());
            if (condition.getMessage() != null) {
              log.info("    Message: " + condition.getMessage());
            }
          });
        }
      }
      
      // Container logs
      log.info("\n--- Container Logs (last 20 lines) ---");
      client.pods().inNamespace(OPERATOR_NAMESPACE).list().getItems().forEach(pod -> {
        log.info("\n=== Logs for pod: " + pod.getMetadata().getName() + " ===");
        
        if (pod.getSpec().getContainers() != null) {
          pod.getSpec().getContainers().forEach(container -> {
            log.info("\n--- Container: " + container.getName() + " ---");
            try {
              String logs = client.pods()
                  .inNamespace(OPERATOR_NAMESPACE)
                  .withName(pod.getMetadata().getName())
                  .inContainer(container.getName())
                  .tailingLines(20)
                  .getLog();
              
              if (logs != null && !logs.trim().isEmpty()) {
                log.info(logs);
              } else {
                log.info("No logs available");
              }
            } catch (Exception e) {
              log.info("Failed to get logs: " + e.getMessage());
            }
          });
        }
      });
      
      log.info("\n=== END DIAGNOSTICS ===\n");
      
    } catch (Exception e) {
      log.error("Failed to collect diagnostics: " + e.getMessage());
    }
  }
}