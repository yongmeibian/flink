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

package org.apache.flink.streaming.connectors.kafka.serializers.registry;

import org.apache.flink.streaming.connectors.kafka.serializers.AbstractAvroSerializationSchema;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;

/**
 * Version of {@link AbstractAvroSerializationSchema} that looks up the writer schema in
 * Confluent's schema registry.
 */
public class ConfluentSchemaRegistryAvroSerializationSchema<T>
	extends AbstractAvroSerializationSchema<T> {

	private final SchemaRegistryClientProvider clientProvider;
	private transient SchemaRegistryClient client;

	/**
	 * Creates a Avro serialization schema which writes object using Confluent's schema registry
	 * and embeds schema version appropriately.
	 *
	 * @param recordClazz type of incoming data. Should be one of:
	 *                    {@link org.apache.avro.specific.SpecificRecord},
	 *                    {@link org.apache.avro.generic.GenericRecord} or a POJO.
	 */
	public ConfluentSchemaRegistryAvroSerializationSchema(
		Class<T> recordClazz,
		SchemaRegistryClientProvider clientProvider) {
		super(recordClazz);
		this.client = clientProvider.get();
		this.clientProvider = clientProvider;
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		this.client = clientProvider.get();
	}

	@Override
	protected void doSerialize(OutputStream out, T record) throws Exception {

		DataOutputStream dataOutputStream = new DataOutputStream(out);
		Schema schema = getSchema();
		int schemaId = client.register(/*TODO where to get topic from*/ "", schema);

		dataOutputStream.writeByte(0);
		dataOutputStream.writeInt(schemaId);
		dataOutputStream.flush();

		GenericDatumWriter<T> datumWriter = getDatumWriter();
		datumWriter.setSchema(schema);

		datumWriter.write(record, getEncoder());
	}
}
