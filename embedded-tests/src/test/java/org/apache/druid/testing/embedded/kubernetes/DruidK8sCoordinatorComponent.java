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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Druid Coordinator service component for managing data availability and segment assignment.
 * Also configured to run as Overlord for task management.
 */
public class DruidK8sCoordinatorComponent extends DruidK8sComponent
{
  private static final String DRUID_COORDINATOR_TEMPLATE = "/data/manifests/druid-components/coordinator.yaml";
  private static final String COORDINATOR_CONFIG_MOUNT_PATH = "/opt/druid/conf/druid/cluster/master/coordinator-overlord";
  private static final int COORDINATOR_READINESS_TIMEOUT = 180;

  public DruidK8sCoordinatorComponent(String namespace, String druidImage, String clusterName)
  {
    super(namespace, druidImage, clusterName);
  }

  @Override
  public String getDruidServiceType()
  {
    return "coordinator";
  }

  @Override
  public int getDruidPort()
  {
    return DRUID_PORT;
  }

  @Override
  public String getRuntimeProperties()
  {
    return "druid.service=druid/coordinator\n" +
           "druid.coordinator.startDelay=PT30S\n" +
           "druid.coordinator.period=PT30S\n" +
           "druid.coordinator.asOverlord.enabled=true\n" +
           "druid.coordinator.asOverlord.overlordService=druid/overlord\n" +
           "druid.indexer.queue.startDelay=PT30S\n" +
           "druid.indexer.runner.capacity=2\n" +
           "druid.indexer.runner.namespace=" + namespace + "\n" +
           "druid.indexer.runner.type=k8s\n" +
           "druid.indexer.task.encapsulatedTask=true\n" +
           "druid.host=druid-" + getMetadataName() + "-" + getDruidServiceType();
  }

  @Override
  public String getJvmOptions()
  {
    return "-server\n" +
           "-Djava.net.preferIPv4Stack=true\n" +
           "-XX:MaxDirectMemorySize=1g\n" +
           "-Duser.timezone=UTC\n" +
           "-Dfile.encoding=UTF-8\n" +
           "-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager\n" +
           "-Xmx800m\n" +
           "-Xms800m";
  }

  @Override
  public void initialize(KubernetesClient client) throws Exception
  {
    applyDruidManifest(client);
  }

  @Override
  public Map<String, Object> getNodeConfig()
  {
    Map<String, Object> nodeConfig = getCommonNodeConfig();
    
    nodeConfig.put("nodeType", "coordinator");
    nodeConfig.put("druid.port", getDruidPort());
    nodeConfig.put("replicas", getReplicas());

    Map<String, Object> serviceSpec = new HashMap<>();
    serviceSpec.put("type", "ClusterIP");
    serviceSpec.put("clusterIP", "None");

    Map<String, Object> httpPort = new HashMap<>();
    httpPort.put("port", getDruidPort());
    httpPort.put("name", "http");
    serviceSpec.put("ports", List.of(httpPort));
    nodeConfig.put("services", List.of(Map.of("spec", serviceSpec)));

    nodeConfig.put("nodeConfigMountPath", COORDINATOR_CONFIG_MOUNT_PATH);
    nodeConfig.put("runtime.properties", getRuntimeProperties().replace(getCommonDruidProperties(), "").trim());
    nodeConfig.put("extra.jvm.options", getJvmOptions());
    return nodeConfig;
  }
  
  @Override
  public String getNodeName()
  {
    return "coordinator";
  }

  @Override
  public int getReadyTimeoutSeconds()
  {
    return COORDINATOR_READINESS_TIMEOUT;
  }

  @Override
  protected String getTemplatePath()
  {
    return DRUID_COORDINATOR_TEMPLATE;
  }
}