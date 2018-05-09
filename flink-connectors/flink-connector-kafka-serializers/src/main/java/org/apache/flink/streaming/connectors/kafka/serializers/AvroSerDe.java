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

package org.apache.flink.streaming.connectors.kafka.serializers;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.serializers.registry.ConfluentSchemaRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.serializers.registry.ConfluentSchemaRegistryAvroSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.serializers.registry.SchemaRegistryClientProvider;
import org.apache.flink.util.Preconditions;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Utility methods for creating Avro (De)SerializationSchema.
 */
public class AvroSerDe {

	public static <T> DeSerializationSchemaBuilder<T> deserialize(Class<T> clazz) {
		return new DeSerializationSchemaBuilder<>(clazz);
	}

	public static <T> SerializationSchemaBuilder<T> serialize(Class<T> clazz) {
		return new SerializationSchemaBuilder<>(clazz);
	}

	public static SchemaRegistryClientBuilder confluentSchemaRegistry() {
		return new SchemaRegistryClientBuilder();
	}

	public static class SerializationSchemaBuilder<T> {

		private final Class<T> clazz;
		private SchemaRegistryClientBuilder schemaRegistryClientBuilder = null;

		SerializationSchemaBuilder(Class<T> clazz) {
			this.clazz = clazz;
		}

		public SerializationSchemaBuilder<T> usingSchemaRegistry(SchemaRegistryClientBuilder schemaRegistryClientBuilder) {
			this.schemaRegistryClientBuilder = schemaRegistryClientBuilder;
			return this;
		}

		public SerializationSchema<T> build() {
			if (schemaRegistryClientBuilder != null) {
				return new ConfluentSchemaRegistryAvroSerializationSchema<>(
					clazz,
					schemaRegistryClientBuilder.build());
			} else {
				return new AvroSerializationSchema<>(clazz);
			}
		}
	}

	/**
	 * Builder for Avro's {@link DeserializationSchema}. It creates either
	 * {@link AvroDeserializationSchema} or {@link ConfluentSchemaRegistryAvroDeserializationSchema}
	 * based on provided configuration.
	 *
	 * @param <T>
	 */
	public static class DeSerializationSchemaBuilder<T> {

		private final Class<T> clazz;
		private Schema reader = null;
		private SchemaRegistryClientBuilder schemaRegistryClientBuilder = null;

		DeSerializationSchemaBuilder(Class<T> clazz) {
			this.clazz = clazz;
		}

		public DeSerializationSchemaBuilder<T> forSchema(Schema reader) {
			this.reader = reader;
			return this;
		}

		public DeSerializationSchemaBuilder<T> usingSchemaRegistry(SchemaRegistryClientBuilder schemaRegistryClientBuilder) {
			this.schemaRegistryClientBuilder = schemaRegistryClientBuilder;
			return this;
		}

		public DeserializationSchema<T> build() {
			if (schemaRegistryClientBuilder != null) {
				return new ConfluentSchemaRegistryAvroDeserializationSchema<>(
					clazz,
					reader,
					schemaRegistryClientBuilder.build());
			} else {
				return new AvroDeserializationSchema<>(clazz, reader);
			}
		}
	}

	/**
	 * Builder for schema registry client.
	 */
	public static class SchemaRegistryClientBuilder {

		private static final String DEFAULT_URL = "http://localhost:8081";
		private static final int DEFAULT_CAPACITY = 1000;

		private String url = DEFAULT_URL;
		private int identityMapCapacity = DEFAULT_CAPACITY;
		private SchemaRegistryClientProvider clientProvider = null;

		public SchemaRegistryClientBuilder registryUrl(String url) {
			Preconditions.checkState(clientProvider == null, "Custom registry client already set!");
			this.url = url;
			return this;
		}

		public SchemaRegistryClientBuilder maxIdentityMapSize(int maxSize) {
			Preconditions.checkState(clientProvider == null, "Custom registry client already set!");
			this.identityMapCapacity = maxSize;
			return this;
		}

		/**
		 * Uses a custom provided schema registry client for communicating with Confluent Schema
		 * Registry.
		 *
		 * @param clientProvider provider for creating the {@link SchemaRegistryClient}.
		 * @return builder with set schema registry
		 */
		public SchemaRegistryClientBuilder customRegistryClient(SchemaRegistryClientProvider clientProvider) {
			Preconditions.checkState(url.equals(DEFAULT_URL), "Registry url already set!");
			Preconditions.checkState(
				identityMapCapacity == DEFAULT_CAPACITY,
				"Registry url already set!");

			this.clientProvider = clientProvider;
			return this;
		}

		SchemaRegistryClientBuilder() {
		}

		private static class CachedSchemaRegistryClientProvider
			implements SchemaRegistryClientProvider {

			private final String url;
			private final int identityMapCapacity;

			private CachedSchemaRegistryClientProvider(String url, int identityMapCapacity) {
				this.url = url;
				this.identityMapCapacity = identityMapCapacity;
			}

			@Override
			public SchemaRegistryClient get() {
				return new CachedSchemaRegistryClient(url, identityMapCapacity);
			}
		}

		SchemaRegistryClientProvider build() {
			if (clientProvider == null) {
				this.clientProvider = new CachedSchemaRegistryClientProvider(
					url,
					identityMapCapacity);
			}

			return clientProvider;
		}
	}

	private AvroSerDe() {
	}
}
