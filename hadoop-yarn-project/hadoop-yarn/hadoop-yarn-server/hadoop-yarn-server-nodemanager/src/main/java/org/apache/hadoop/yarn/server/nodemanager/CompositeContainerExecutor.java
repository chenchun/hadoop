/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompositeContainerExecutor extends ContainerExecutor {
  private static final Log LOG = LogFactory
      .getLog(CompositeContainerExecutor.class);

  private Map<String, ContainerExecutor> executorMap;
  private ContainerExecutor defaultExec;
  private Context context;
  private String validContainerExecutor;

  public CompositeContainerExecutor(Map<String, ContainerExecutor> executorMap,
      ContainerExecutor defaultExec, Context context) {
    Preconditions.checkNotNull(executorMap);
    this.executorMap = executorMap;
    this.validContainerExecutor = Joiner.on(",").join(executorMap.keySet());
    this.defaultExec = defaultExec;
    this.context = context;
  }

  public CompositeContainerExecutor(ContainerExecutor exec) {
    this(ImmutableMap.of(exec.getClass().getName(), exec), exec, null);
  }

  @Override
  public void init() throws IOException {
    for (ContainerExecutor exec : executorMap.values()) {
        exec.init();
    }
  }

  @Override
  public synchronized void startLocalizer(Path nmPrivateContainerTokensPath,
      InetSocketAddress nmAddr, String user, String appId, String locId,
      List<String> localDirs, List<String> logDirs) {
    throw new YarnRuntimeException("Should never call this");
  }

  @Override
  public int launchContainer(Container container,
      Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
      String userName, String appId, Path containerWorkDir,
      List<String> localDirs, List<String> logDirs) throws IOException {
    throw new YarnRuntimeException("Should never call this");
  }

  @Override
  public boolean signalContainer(String user, String pid, Signal signal)
      throws IOException {
    throw new YarnRuntimeException("Should never call this");
  }

  @Override
  public void deleteAsUser(String user, Path subDir, Path... baseDirs)
      throws IOException, InterruptedException {
    for (ContainerExecutor exec : executorMap.values()) {
        exec.deleteAsUser(user, subDir, baseDirs);
    }
  }

  public boolean isValidContainerExecutor(String execClassName) {
    return execClassName == null || execClassName.trim().isEmpty() ||
        executorMap.containsKey(execClassName);
  }

  public ContainerExecutor getContainerExecutor(
      ContainerLaunchContext containerLaunchContext) {
    if (containerLaunchContext == null || containerLaunchContext
        .getContainerExecutor() == null || containerLaunchContext
        .getContainerExecutor().trim().isEmpty()) {
      return defaultExec;
    } else {
      return executorMap.get(containerLaunchContext.getContainerExecutor());
    }
  }

  public ContainerExecutor getContainerExecutor(ContainerId containerId) {
    ContainerExecutor exec;
    if (context != null) {
      Container container = context.getContainers().get(containerId);
      if (container == null) {
        exec = defaultExec;
      } else {
        exec = getContainerExecutor(container.getLaunchContext());
      }
    } else {
      exec = defaultExec;
    }
    exec = exec == null? defaultExec : exec;
    LOG.info("Launch container " + containerId.toString() + " with " + exec
        .getClass().getName());
    return exec;
  }

  public String getValidContainerExecutor() {
    return this.validContainerExecutor;
  }
}
