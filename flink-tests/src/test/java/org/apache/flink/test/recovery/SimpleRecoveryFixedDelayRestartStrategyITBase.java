/*
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

package org.apache.flink.test.recovery;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.category.AlsoRunWithSchedulerNG;

import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

/**
 * Test cluster configuration with fixed-delay recovery.
 */
@Category(AlsoRunWithSchedulerNG.class)
public class SimpleRecoveryFixedDelayRestartStrategyITBase extends SimpleRecoveryITCaseBase {

	@ClassRule
	public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE = new MiniClusterWithClientResource(
		new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(getConfiguration())
			.setNumberTaskManagers(2)
			.setNumberSlotsPerTaskManager(2)
			.build());

	private static Configuration getConfiguration() {
		Configuration config = new Configuration();
		config.setString(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
		config.setInteger(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
		config.setString(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY.key(), "100 ms");

		return config;
	}
}
