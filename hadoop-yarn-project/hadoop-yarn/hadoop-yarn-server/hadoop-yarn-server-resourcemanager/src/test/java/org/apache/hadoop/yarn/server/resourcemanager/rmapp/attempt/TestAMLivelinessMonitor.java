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
package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestAMLivelinessMonitor {

  @Test(timeout = 20000)
  public void testAMLivelinessMonitor()
      throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    UserGroupInformation.setConfiguration(conf);
    conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    conf.setInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 6000);
    MemoryRMStateStore memStore = new MemoryRMStateStore() {
      @Override
      public synchronized RMState loadState() throws Exception {
        Thread.sleep(8000);
        return super.loadState();
      }
    };
    memStore.init(conf);
    final ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance
        (ApplicationId.newInstance(System.currentTimeMillis(), 1) , 1);
    final Dispatcher dispatcher = mock(Dispatcher.class, RETURNS_DEEP_STUBS);
    when(dispatcher.getEventHandler()).thenReturn(null);
    final boolean[] fail = new boolean[]{false};
    final AMLivelinessMonitor monitor = new AMLivelinessMonitor(
        dispatcher) {
      @Override
      protected void expire(ApplicationAttemptId id) {
        Assert.assertEquals(id, attemptId);
        fail[0] = true;
      }
    };
    monitor.register(attemptId);
    MockRM rm = new MockRM(conf, memStore) {
      @Override
      protected AMLivelinessMonitor createAMLivelinessMonitor() {
        return monitor;
      }
    };
    rm.start();
    Assert.assertEquals(Service.STATE.STARTED, monitor.getServiceState());
    Assert.assertFalse(fail[0]);
  }
}
