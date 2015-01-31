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
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.TestContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCompositeContainerExecutor extends BaseContainerManagerTest {

  public TestCompositeContainerExecutor() throws
      UnsupportedFileSystemException {
    super();
  }

  @Override
  @Before
  public void setup() throws IOException {
    super.setup();
    ((NodeManager.NMContext) context).setCompositeContainerExecutor(exec);
  }

  @Override
  protected CompositeContainerExecutor createContainerExecutor() {
    conf.set(YarnConfiguration.NM_CONTAINER_EXECUTOR,
        ValidContainerExecutor.class.getName() + "," +
            DefaultContainerExecutor.class.getName());
    conf.set(YarnConfiguration.NM_DEFAULT_CONTAINER_EXECUTOR,
        ValidContainerExecutor.class.getName());
    return new CompositeContainerExecutor(conf, context);
  }

  public static final class ValidContainerExecutor extends DefaultContainerExecutor {
    int launchCount = 0;

    @Override
    public int launchContainer(Container container,
        Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
        String user, String appId, Path containerWorkDir,
        List<String> localDirs, List<String> logDirs) throws IOException {
      launchCount++;
      return super.launchContainer(container, nmPrivateContainerScriptPath,
          nmPrivateTokensPath, user, appId, containerWorkDir, localDirs,
          logDirs);
    }
  }

  @Test
  public void testContainerExecutorConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();

    //test single container executor configuration
    conf.setClass(YarnConfiguration.NM_CONTAINER_EXECUTOR,
        ValidContainerExecutor.class, ContainerExecutor.class);
    CompositeContainerExecutor compositeExec = new CompositeContainerExecutor
        (conf, null);
    expectExecutors(compositeExec, ValidContainerExecutor.class.getName(),
        ValidContainerExecutor.class, ValidContainerExecutor.class);

    //test multiple container executor configuration
    conf.set(YarnConfiguration.NM_CONTAINER_EXECUTOR,
        ValidContainerExecutor.class.getName() + "," +
            DefaultContainerExecutor.class.getName());
    boolean expectFail = false;
    try {
      new CompositeContainerExecutor(conf, null);
    } catch (Exception e) {
      //Do not configure NM_DEFAULT_CONTAINER_EXECUTOR, should get an exception
      Assert.assertEquals("Need to make a configuration for " +
          YarnConfiguration.NM_DEFAULT_CONTAINER_EXECUTOR + " since " +
          YarnConfiguration.NM_CONTAINER_EXECUTOR + "contains multiple " +
          "values.", e.getMessage());
      expectFail = true;
    }
    Assert.assertTrue(expectFail);
  }

  @SafeVarargs
  private final void expectExecutors(CompositeContainerExecutor compositeExec,
      String validExec, Class<? extends ContainerExecutor> defaultExec,
      Class<? extends ContainerExecutor>... expectExecs) {
    Assert.assertEquals(validExec, compositeExec.validContainerExecutor);
    Assert.assertEquals(defaultExec, compositeExec.defaultExec.getClass());
    if (expectExecs != null) {
      Assert.assertEquals(expectExecs.length, compositeExec.execMap.size());
      for (Class<? extends ContainerExecutor> clazz : expectExecs) {
        Assert.assertTrue(compositeExec.execMap.containsKey(clazz.getName()));
        Assert.assertEquals(clazz, compositeExec.execMap.get(clazz.getName()).getClass());
      }
    }
  }

  @Test
  public void testWithMocks() {
    expectExecutors(exec, Joiner.on(",")
            .join(ValidContainerExecutor.class.getName(),
                DefaultContainerExecutor.class.getName()),
        ValidContainerExecutor.class, ValidContainerExecutor.class,
        DefaultContainerExecutor.class);
    ContainerLaunchContext launchContext = mock(ContainerLaunchContext.class);
    ContainerId containerId = createContainerId(0);
    Container container = mock(Container.class);
    when(container.getContainerId()).thenReturn(containerId);
    when(container.getLaunchContext()).thenReturn(launchContext);
    context.getContainers().put(containerId, container);

    when(launchContext.getContainerExecutor())
        .thenReturn(DefaultContainerExecutor.class.getName());
    Assert.assertEquals(DefaultContainerExecutor.class,
        exec.getContainerExecutor(containerId).getClass());

    when(launchContext.getContainerExecutor())
        .thenReturn(ValidContainerExecutor.class.getName());
    Assert.assertEquals(ValidContainerExecutor.class,
        exec.getContainerExecutor(containerId).getClass());

    // Do not have LinuxContainerExecutor configured, should fail
    when(launchContext.getContainerExecutor())
        .thenReturn(LinuxContainerExecutor.class.getName());
    boolean expectFail = false;
    try {
      exec.getContainerExecutor(containerId);
    } catch (Exception e) {
      Assert.assertEquals(YarnRuntimeException.class, e.getClass());
      Assert.assertEquals(exec.getInValidExecLog(containerId,
          LinuxContainerExecutor.class.getName()), e.getMessage());
      expectFail = true;
    }
    Assert.assertTrue(expectFail);

    ContainerId notExistContainer = createContainerId(1);
    Assert.assertEquals(ValidContainerExecutor.class,
        exec.getContainerExecutor(notExistContainer).getClass());
  }

  @Test
  public void testLaunchContainers() throws IOException, YarnException,
      InterruptedException {
    ValidContainerExecutor validExec = (ValidContainerExecutor) exec.execMap
        .get(ValidContainerExecutor.class.getName());
    containerManager.start();
    ContainerId cId = createContainerId(0);
    StartContainerRequest scRequest = createStartContainerRequest(cId,
        LinuxContainerExecutor.class);
    Assert.assertEquals(0, validExec.launchCount);

    StartContainersResponse scResponse = containerManager.startContainers(
        StartContainersRequest.newInstance(ImmutableList.of(scRequest)));
    Assert.assertEquals(1, scResponse.getFailedRequests().size());
    Map<ContainerId, SerializedException> failedRequests = scResponse
        .getFailedRequests();
    Assert.assertTrue(failedRequests.containsKey(cId));
    Throwable throwable = failedRequests.get(cId).deSerialize();
    Assert.assertEquals(YarnException.class, throwable.getClass());
    Assert.assertEquals(exec.getInValidExecLog(cId, LinuxContainerExecutor
        .class.getName()), throwable.getMessage());

    //container with ValidContainerExecutor should be launched with ValidContainerExecutor
    Assert.assertEquals(0, validExec.launchCount);
    cId = createContainerId(1);
    scRequest = createStartContainerRequest(cId, ValidContainerExecutor.class);
    containerManager.startContainers(
        StartContainersRequest.newInstance(ImmutableList.of(scRequest)));
    BaseContainerManagerTest
        .waitForContainerState(containerManager, cId, ContainerState.COMPLETE);
    Assert.assertEquals(1, validExec.launchCount);

    //container with DefaultContainerExecutor should be launched with DefaultContainerExecutor
    cId = createContainerId(2);
    scRequest = createStartContainerRequest(cId, DefaultContainerExecutor.class);
    containerManager.startContainers(
        StartContainersRequest.newInstance(ImmutableList.of(scRequest)));
    BaseContainerManagerTest
        .waitForContainerState(containerManager, cId, ContainerState.COMPLETE);
    Assert.assertEquals(1, validExec.launchCount);
  }

  private StartContainerRequest createStartContainerRequest(ContainerId cId,
      Class<? extends ContainerExecutor> clazz) throws IOException {
    ContainerLaunchContext containerLaunchContext = recordFactory
        .newRecordInstance(ContainerLaunchContext.class);
    containerLaunchContext.setContainerExecutor(clazz.getName());
    return StartContainerRequest.newInstance(containerLaunchContext,
        TestContainerManager
            .createContainerToken(cId, DUMMY_RM_IDENTIFIER, context.getNodeId(),
                user, context.getContainerTokenSecretManager()));
  }

  private ContainerId createContainerId(int id) {
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId
        .newInstance(appId, 1);
    return ContainerId.newContainerId(appAttemptId, id);
  }
}
