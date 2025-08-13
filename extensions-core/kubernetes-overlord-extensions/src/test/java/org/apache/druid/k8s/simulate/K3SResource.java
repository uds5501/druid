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

package org.apache.druid.k8s.simulate;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A K3s container for use in embedded tests.
 */
public class K3SResource extends TestcontainerResource<K3sContainer>
{

  private static final String K3S_IMAGE = "rancher/k3s:v1.28.8-k3s1";

  private EmbeddedDruidCluster cluster;
  private KubernetesClient client;

  @Override
  public void beforeStart(EmbeddedDruidCluster cluster)
  {
    this.cluster = cluster;
  }


  @Override
  protected K3sContainer createContainer()
  {
    K3sContainer container = new K3sContainer(DockerImageName.parse(K3S_IMAGE))
        .withCreateContainerCmdModifier(cmd -> {
          cmd.withPlatform("linux/amd64");
        });
    container.start();
    this.client = new KubernetesClientBuilder()
        .withConfig(Config.fromKubeconfig(container.getKubeConfigYaml()))
        .build();
    return container;
  }

  public KubernetesClient getClient()
  {
    return client;
  }
}
