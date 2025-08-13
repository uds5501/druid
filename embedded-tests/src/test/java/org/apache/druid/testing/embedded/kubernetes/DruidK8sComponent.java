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

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Base class for all Druid Kubernetes components. Provides common functionality
 * for Druid services running on Kubernetes with configurable images and RBAC.
 */
public abstract class DruidK8sComponent implements K8sComponent
{
  private static final Logger log = new Logger(DruidK8sComponent.class);
  private static final int POD_READY_CHECK_TIMEOUT_SECONDS = 120;
  public static final int DRUID_PORT = 8088;

  protected final String namespace;
  protected final String druidImage;
  protected final String clusterName;

  public static final String DRUID_34_IMAGE = "apache/druid:34.0.0";

  protected DruidK8sComponent(String namespace, String druidImage, String clusterName)
  {
    this.namespace = namespace;
    this.druidImage = druidImage != null ? druidImage : DRUID_34_IMAGE;
    this.clusterName = clusterName;
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

  public abstract String getDruidServiceType();

  /**
   * Get the Druid port for this service.
   *
   * @return the port number
   */
  public abstract int getDruidPort();

  /**
   * Get the runtime properties specific to this Druid service.
   *
   * @return the runtime properties as a string
   */
  public abstract String getRuntimeProperties();

  /**
   * Get the JVM options for this service.
   * 
   * @return the JVM options
   */
  public abstract String getJvmOptions();
  
  /**
   * Get the node configuration for this service to be used in the Druid YAML.
   * 
   * @return a map representing the node configuration
   */
  public abstract Map<String, Object> getNodeConfig();

  protected Map<String, Object> getCommonNodeConfig()
  {
    Map<String, Object> nodeConfig = new HashMap<>();

    Map<String, Object> serviceSpec = new HashMap<>();
    serviceSpec.put("type", "ClusterIP");
    serviceSpec.put("clusterIP", "None");
    nodeConfig.put("services", List.of(Map.of("spec", serviceSpec)));

    return nodeConfig;
  }

  public int getReplicas()
  {
    return 1;
  }

  public int getReadyTimeoutSeconds()
  {
    return POD_READY_CHECK_TIMEOUT_SECONDS;
  }

  /**
   * Get common Druid configuration that applies to all services.
   * For the first cut, this is static
   * 
   */
  protected String getCommonDruidProperties()
  {
    return String.format(
        "# Zookeeper-less Druid Cluster\n" +
        "druid.zk.service.enabled=false\n" +
        "druid.discovery.type=k8s\n" +
        "druid.discovery.k8s.clusterIdentifier=%s\n" +
        "druid.serverview.type=http\n" +
        "druid.coordinator.loadqueuepeon.type=http\n" +
        "druid.indexer.runner.type=httpRemote\n" +
        "\n" +
        "# Metadata Store (Derby for testing)\n" +
        "druid.metadata.storage.type=derby\n" +
        "druid.metadata.storage.connector.connectURI=jdbc:derby://localhost:1527/var/druid/metadata.db;create=true\n" +
        "druid.metadata.storage.connector.host=localhost\n" +
        "druid.metadata.storage.connector.port=1527\n" +
        "druid.metadata.storage.connector.createTables=true\n" +
        "\n" +
        "# Extensions\n" +
        "druid.extensions.loadList=[\"druid-kubernetes-overlord-extensions\", \"druid-kubernetes-extensions\", \"druid-datasketches\"]\n" +
        "\n" +
        "# Service discovery\n" +
        "druid.selectors.indexing.serviceName=druid/overlord\n" +
        "druid.selectors.coordinator.serviceName=druid/coordinator\n" +
        "\n" +
        "# Indexing logs\n" +
        "druid.indexer.logs.type=file\n",
        clusterName
    );
  }

  /**
   * Apply the Druid YAML manifest for this component.
   */
  public void applyDruidManifest(KubernetesClient client)
  {
    String templatePath = getTemplatePath();
    InputStream yamlStream = getClass().getResourceAsStream(templatePath);
    if (yamlStream == null) {
      throw new RuntimeException("Could not find Druid YAML template: " + templatePath);
    }
    GenericKubernetesResource druidResource = Serialization.unmarshal(yamlStream, GenericKubernetesResource.class);
    customizeDruidResource(druidResource);
    client.resource(druidResource).inNamespace(namespace).create();
  }
  
  /**
   * Customize the Druid resource YAML for this specific component.
   */
  protected void customizeDruidResource(GenericKubernetesResource druidResource)
  {
    Map<String, Object> spec = (Map<String, Object>) druidResource.getAdditionalProperties().get("spec");
    
    spec.put("image", druidImage);
    druidResource.getMetadata().setName(getMetadataName());
    druidResource.getMetadata().setNamespace(namespace);
    
    String commonProperties = (String) spec.get("common.runtime.properties");
    if (commonProperties != null) {
      commonProperties = commonProperties.replace("druid-it", clusterName)
          .replace("druid.indexer.runner.namespace: druid", "druid.indexer.runner.namespace: " + namespace);
      spec.put("common.runtime.properties", commonProperties);
    }
    Map<String, Object> nodes = new HashMap<>();
    nodes.put(getNodeName(), getNodeConfig());
    spec.put("nodes", nodes);
  }
  
  /**
   * Get the metadata name for this Druid custom resource.
   * 
   * @return the metadata name
   */
  protected String getMetadataName()
  {
    return clusterName + "-" + getDruidServiceType();
  }
  
  /**
   * Get the node name for this service type in the YAML configuration.
   * 
   * @return the node name
   */
  public String getNodeName()
  {
    return getDruidServiceType();
  }
  
  /**
   * Get the component-specific template path.
   * 
   * @return the template path for this component
   */
  abstract protected String getTemplatePath();


  @Override
  public void waitUntilReady(KubernetesClient client)
  {
    try {
      String labelValue = getPodLabel();
      String componentName = getMetadataName();

      client.pods()
          .inNamespace(namespace)
          .withLabel("druid_cr", componentName)
          .withLabel("nodeSpecUniqueStr", labelValue)
          .waitUntilReady(getReadyTimeoutSeconds(), TimeUnit.SECONDS);
    } catch (Exception e) {
      log.error("Timeout waiting for Druid %s to be ready", getDruidServiceType());
      printDruidDiagnostics(client);
      throw e;
    }
  }

  protected String getPodLabel()
  {
    return "druid-" + getMetadataName() + "-" + getDruidServiceType();
  }

  @Override
  public void cleanup(KubernetesClient client)
  {
    try {
      client.genericKubernetesResources("druid.apache.org/v1alpha1", "Druid")
          .inNamespace(namespace)
          .withName(getMetadataName())
          .delete();
    } catch (Exception e) {
      log.error("Error during Druid %s cleanup: %s", getDruidServiceType(), e.getMessage());
    }
  }

  protected void printDruidDiagnostics(KubernetesClient client)
  {
    try {
      log.info("=== DRUID %s DIAGNOSTICS ===", getDruidServiceType().toUpperCase());
      
      // Pod status
      log.info("--- Pod Status ---");
      String labelValue = getDruidServiceType();
      if ("historical".equals(getDruidServiceType())) {
        labelValue = ((DruidK8sHistoricalComponent) this).getTier();
      }
      
      String componentName = getMetadataName();
      client.pods().inNamespace(namespace)
          .withLabel("app.kubernetes.io/name", "druid")
          .withLabel("druid_cr", componentName)
          .withLabel("nodeSpecUniqueStr", labelValue)
          .list().getItems().forEach(pod -> {
        log.info("Pod: %s", pod.getMetadata().getName());
        log.info("  Status: %s", pod.getStatus().getPhase());
        
        if (pod.getStatus().getContainerStatuses() != null) {
          pod.getStatus().getContainerStatuses().forEach(containerStatus -> {
            log.info("  Container: %s", containerStatus.getName());
            log.info("    Ready: %s", containerStatus.getReady());
            log.info("    Restart Count: %s", containerStatus.getRestartCount());
            
            if (containerStatus.getState() != null) {
              if (containerStatus.getState().getWaiting() != null) {
                log.info("    State: Waiting - %s", containerStatus.getState().getWaiting().getReason());
              } else if (containerStatus.getState().getTerminated() != null) {
                log.info("    State: Terminated - %s", containerStatus.getState().getTerminated().getReason());
                log.info("    Exit Code: %s", containerStatus.getState().getTerminated().getExitCode());
              }
            }
          });
        }
      });
      
      log.info("--- Recent Logs ---");
      if ("historical".equals(getDruidServiceType())) {
        labelValue = ((DruidK8sHistoricalComponent) this).getTier();
      }
      
      client.pods().inNamespace(namespace)
          .withLabel("app.kubernetes.io/name", "druid")
          .withLabel("druid_cr", componentName)
          .withLabel("nodeSpecUniqueStr", labelValue)
          .list().getItems().forEach(pod -> {
        try {
          String logs = client.pods()
              .inNamespace(namespace)
              .withName(pod.getMetadata().getName())
              .tailingLines(10)
              .getLog();
          
          if (logs != null && !logs.trim().isEmpty()) {
            log.info("Pod %s logs:\n%s", pod.getMetadata().getName(), logs);
          }
        } catch (Exception e) {
          log.warn("Could not get logs for pod %s", pod.getMetadata().getName());
        }
      });
      
      log.info("=== END DIAGNOSTICS ===");
      
    } catch (Exception e) {
      log.error("Failed to collect diagnostics: %s", e.getMessage());
    }
  }

  @Override
  public String getComponentName()
  {
    return "Druid" + getDruidServiceType().substring(0, 1).toUpperCase() + getDruidServiceType().substring(1);
  }
}