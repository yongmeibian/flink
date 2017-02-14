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
package org.apache.flink.runtime.webmonitor.handlers;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.query.KvStateID;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateMessage;
import org.apache.flink.runtime.query.KvStateServerAddress;
import org.apache.flink.util.StringUtils;
import scala.concurrent.Await;
import scala.concurrent.duration.FiniteDuration;
import scala.reflect.ClassTag$;

import java.io.StringWriter;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class KvStateLocationLookupHandler extends AbstractJsonRequestHandler {

	private final FiniteDuration timeout;

	public KvStateLocationLookupHandler(FiniteDuration timeout) {
		this.timeout = requireNonNull(timeout);
	}

	@Override
	public String handleJsonRequest(Map<String, String> pathParams,
									Map<String, String> queryParams,
									ActorGateway jobManager) throws Exception {
		try {
			if (jobManager != null) {
				final JobID jobId = new JobID(StringUtils.hexStringToByte(pathParams.get("jobid")));
				final String registrationName = pathParams.get("registrationName");
				final Object msg = new KvStateMessage.LookupKvStateLocation(jobId, registrationName);
				final KvStateLocation kvStateLocation = Await.result(
					jobManager.ask(msg, timeout).mapTo(ClassTag$.MODULE$.<KvStateLocation>apply(KvStateLocation
						.class)),
					timeout);

				final StringWriter writer = new StringWriter();
				final JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

				gen.writeStartObject();

				gen.writeStringField("jobId", kvStateLocation.getJobId().toString());
				gen.writeStringField("jobVertexId", kvStateLocation.getJobVertexId().toString());
				gen.writeNumberField("numKeyGroups", kvStateLocation.getNumKeyGroups());
				gen.writeStringField("registrationName", kvStateLocation.getRegistrationName());

				gen.writeArrayFieldStart("kvStates");

				for (int i = 0; i < kvStateLocation.getNumKeyGroups(); i++) {

					final KvStateID kvStateID = kvStateLocation.getKvStateID(i);
					final KvStateServerAddress kvStateServerAddress = kvStateLocation.getKvStateServerAddress(i);

					if (kvStateID != null && kvStateServerAddress != null) {
						gen.writeStartObject();

						gen.writeNumberField("keyGroup", i);

						gen.writeStringField("id", kvStateID.toString());

						gen.writeObjectFieldStart("serverAddress");
						gen.writeStringField("hostAddress",
							kvStateServerAddress.getHost().getCanonicalHostName());
						gen.writeNumberField("port", kvStateServerAddress.getPort());
						gen.writeEndObject();

						gen.writeEndObject();
					}
				}

				gen.writeEndArray();

				gen.writeEndObject();

				gen.close();
				return writer.toString();
			} else {
				throw new Exception("No connection to the leading JobManager.");
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to fetch kv state locations: " + e.getMessage(), e);
		}
	}
}
