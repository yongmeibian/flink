/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.gcp.pubsub.common;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import com.google.pubsub.v1.PubsubMessage;

import java.io.Serializable;

/**
 * The deserialization schema describes how to turn the PubsubMessages
 * into data types (Java/Scala objects) that are processed by Flink.
 *
 * @param <T> The type created by the deserialization schema.
 */
public interface PubSubDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

	/**
	 * Initialization method for the schema. It is called before the actual working methods
	 * {@link #deserialize} and thus suitable for one time setup work.
	 *
	 * <p>The provided {@link DeserializationSchema.InitializationContext} can be used to access additional features such as e.g.
	 * registering user metrics.
	 *
	 * @param context Contextual information that can be used during initialization.
	 */
	@PublicEvolving
	default void open(DeserializationSchema.InitializationContext context) throws Exception {
	}

	/**
	 * Method to decide whether the element signals the end of the stream. If
	 * true is returned the element won't be emitted.
	 *
	 * @param nextElement The element to test for the end-of-stream signal.
	 *
	 * @return True, if the element signals end of stream, false otherwise.
	 */
	boolean isEndOfStream(T nextElement);

	/**
	 * Deserializes a PubsubMessage.
	 *
	 * @param message PubsubMessage to be deserialized.
	 *
	 * @return The deserialized message as an object (null if the message cannot be deserialized).
	 */
	T deserialize(PubsubMessage message) throws Exception;

	/**
	 * Deserializes a PubsubMessage.
	 *
	 * <p>Can output multiple records through the {@link Collector}. Note that number and size of the
	 * produced records should be relatively small. Depending on the source implementation records
	 * can be buffered in memory or collecting records might delay emitting checkpoint barrier.
	 *
	 * @param message The message, as a byte array.
	 * @param out The collector to put the resulting messages.
	 */
	@PublicEvolving
	default void deserialize(PubsubMessage message, Collector<T> out) throws Exception {
		T deserialize = deserialize(message);
		if (deserialize != null) {
			out.collect(deserialize);
		}
	}

	/**
	 * Tear-down method for the user code. It is called after all messages has been processed.
	 * After this method is called there will be no more invocations to {@link #deserialize}.
	 */
	@PublicEvolving
	default void close() throws Exception {
	}
}
