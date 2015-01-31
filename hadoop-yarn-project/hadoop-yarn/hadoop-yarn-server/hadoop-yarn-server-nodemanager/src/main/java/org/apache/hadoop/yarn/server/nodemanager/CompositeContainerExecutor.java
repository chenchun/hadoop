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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompositeContainerExecutor extends ContainerExecutor {
  private static final Log LOG = LogFactory
      .getLog(CompositeContainerExecutor.class);

  @VisibleForTesting
  Map<String, ContainerExecutor> execMap;
  @VisibleForTesting
  ContainerExecutor defaultExec;
  @VisibleForTesting
  String validContainerExecutor;
  private Context context;

  @SuppressWarnings("unchecked")
  public CompositeContainerExecutor(Configuration conf, Context context) {
    String execClasses = conf.get(YarnConfiguration
        .NM_CONTAINER_EXECUTOR, DefaultContainerExecutor.class.getName());
    String defaultExecClass = !execClasses.contains(",")? execClasses : conf
        .get(YarnConfiguration.NM_DEFAULT_CONTAINER_EXECUTOR);
    if (defaultExecClass == null) {
      throw new YarnRuntimeException("Need to make a configuration for " +
          YarnConfiguration.NM_DEFAULT_CONTAINER_EXECUTOR + " since " +
          YarnConfiguration.NM_CONTAINER_EXECUTOR + "contains multiple " +
          "values.");
    }
    this.execMap = new HashMap<String, ContainerExecutor>();
    Class<? extends ContainerExecutor> execClass;
    for (String containerExecutor : Splitter.on(',').omitEmptyStrings()
        .trimResults().split(execClasses)) {
      try {
        execClass = (Class<? extends ContainerExecutor>) conf
            .getClassByName(containerExecutor);
      } catch (ClassNotFoundException e) {
        throw new YarnRuntimeException("Failed to find container executor " +
            "class " + containerExecutor);
      } catch (ClassCastException e) {
        throw new YarnRuntimeException("Container executor class " +
            containerExecutor + " should extend " + ContainerExecutor.class
            .getName());
      }
      if (!execMap.containsKey(containerExecutor)) {
        ContainerExecutor exec = ReflectionUtils.newInstance(execClass, conf);
        execMap.put(containerExecutor, exec);
        if (containerExecutor.equals(defaultExecClass)) {
          this.defaultExec = exec;
        }
      }
    }
    this.validContainerExecutor = Joiner.on(",").join(execMap.keySet());
    this.context = context;
  }

  public CompositeContainerExecutor(ContainerExecutor exec) {
    this.execMap = ImmutableMap.of(exec.getClass().getName(), exec);
    this.validContainerExecutor = exec.getClass().getName();
    this.defaultExec = exec;
    this.context = null;
  }

  @Override
  public void init() throws IOException {
    for (ContainerExecutor exec : execMap.values()) {
      exec.init();
    }
  }

  @Override
  public void startLocalizer(Path nmPrivateContainerTokens,
      InetSocketAddress nmAddr, String user, String appId, String locId,
      LocalDirsHandlerService dirsHandler) throws IOException,
      InterruptedException {
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
  public boolean signalContainer(String user, String pid, Signal signal) throws
      IOException {
    throw new YarnRuntimeException("Should never call this");
  }

  @Override
  public boolean isContainerProcessAlive(String user, String pid) throws
      IOException {
    throw new YarnRuntimeException("Should never call this");
  }

  @Override
  public void deleteAsUser(String user, Path subDir, Path... baseDirs) throws
      IOException, InterruptedException {
    for (ContainerExecutor exec : execMap.values()) {
      exec.deleteAsUser(user, subDir, baseDirs);
    }
  }

  public ContainerExecutor getContainerExecutor(ContainerId containerId) {
    ContainerExecutor exec = null;
    if (context != null) {
      Container container = context.getContainers().get(containerId);
      if (container == null) {
        LOG.warn("Can't find container " + containerId + ", " +
            "use " + defaultExec.getClass() + " instead.");
        exec = defaultExec;
      } else {
        ContainerLaunchContext launchContext = container.getLaunchContext();
        if (launchContext == null || launchContext
            .getContainerExecutor() == null || launchContext
            .getContainerExecutor().trim().isEmpty()) {
          exec = defaultExec;
        } else {
          exec = execMap.get(launchContext.getContainerExecutor());
          if (exec == null) {
            throw new YarnRuntimeException(this.getInValidExecLog
                (containerId, launchContext.getContainerExecutor()));
          }
        }
      }
    }
    exec = exec == null? defaultExec : exec;
    LOG.info("Launch container " + containerId.toString() + " using " + exec
        .getClass().getName());
    return exec;
  }

  public boolean isValidContainerExecutor(String execClassName) {
    return execClassName == null || execMap.containsKey(execClassName);
  }

  public String getInValidExecLog(ContainerId containerId, String execName) {
    return "Container " + containerId + " with invalid container executor " +
        execName + ", please use " + this.validContainerExecutor + " instead.";
  }
}