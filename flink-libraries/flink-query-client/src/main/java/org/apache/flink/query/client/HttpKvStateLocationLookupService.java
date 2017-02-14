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
package org.apache.flink.query.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.impl.FlinkFuture;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

class HttpKvStateLocationLookupService implements KvStateLocationLookupService {

	private static final Logger LOG = LoggerFactory.getLogger(KvStateLocationLookupService.class);

	private final CloseableHttpClient client;

	private final String jobManagerAddress;

	private final Executor executor;

	@Override
	public void start() {
	}

	@Override
	public void shutDown() {
		try {
			client.close();
		} catch (Exception e) {
			LOG.error("Failed to stop http client", e);
			throw new RuntimeException(e);
		}
	}

	public HttpKvStateLocationLookupService(final String jobManagerAddress, final Executor executor) {
		this.jobManagerAddress = jobManagerAddress;
		this.executor = executor;
		client = HttpClientBuilder.create().build();
	}

	@Override
	public Future<KvStateLocation> getKvStateLookupInfo(final JobID jobId, final String registrationName) {
		return ((FlinkFuture<KvStateLocation>) FlinkFuture.supplyAsync(new Callable<KvStateLocation>() {
			@Override
			public KvStateLocation call() throws Exception {

				final String path = "jobs/" + jobId + "/state-location-lookup/" + registrationName;
				final URIBuilder uriBuilder = new URIBuilder(jobManagerAddress);
				uriBuilder.setScheme("http").setPath(path);
				final HttpGet get = new HttpGet(uriBuilder.build());
				final HttpResponse response = client.execute(get);


				return null;
			}
		}, executor)).getScalaFuture();
	}
}
