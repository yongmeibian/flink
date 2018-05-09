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

import org.apache.flink.api.common.serialization.ConsumerRecordMetaInfo;
import org.apache.flink.streaming.connectors.kafka.serializers.AbstractAvroDeserializationSchema;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

/**
 * Version of {@link AbstractAvroDeserializationSchema} that looks up the writer schema in
 * Confluent's schema registry.
 */
public class ConfluentSchemaRegistryAvroDeserializationSchema<T>
	extends AbstractAvroDeserializationSchema<T> {

	private transient SchemaRegistryClient registryClient;

	private SchemaRegistryClientProvider clientProvider;

	/**
	 * Creates an Avro deserialization schema which reads object using Confluent's schema registry
	 * and embedded schema version appropriately.
	 *
	 * @param recordClazz type of incoming data. Should be one of:
	 *                    {@link org.apache.avro.specific.SpecificRecord},
	 *                    {@link org.apache.avro.generic.GenericRecord} or a POJO.
	 */
	public ConfluentSchemaRegistryAvroDeserializationSchema(
		Class<T> recordClazz,
		Schema reader,
		SchemaRegistryClientProvider registryClientProvider) {
		super(recordClazz, reader);
		this.clientProvider = registryClientProvider;
		this.registryClient = registryClientProvider.get();
	}

	@Override
	protected T doDeserialize(
		InputStream inputStream, ConsumerRecordMetaInfo consumerRecordMetaInfo) throws Exception {

		Schema writerSchema = getWriterSchema(inputStream, consumerRecordMetaInfo);
		Schema readerSchema = getReaderSchema();
		GenericDatumReader<T> datumReader = getDatumReader();

		datumReader.setSchema(writerSchema);
		datumReader.setExpected(readerSchema);

		return datumReader.read(null, getDecoder());
	}

	private Schema getWriterSchema(
		InputStream in,
		ConsumerRecordMetaInfo consumerRecordMetaInfossage) throws Exception {

		DataInputStream dataInputStream = new DataInputStream(in);

		if (dataInputStream.readByte() != 0) {
			throw new RuntimeException("Unknown data format. Magic number does not match");
		} else {
			String topic = consumerRecordMetaInfossage.getTopic();
			int schemaId = dataInputStream.readInt();

			if (topic != null) {
				return registryClient.getBySubjectAndId(topic, schemaId);
			} else {
				return registryClient.getById(schemaId);
			}
		}
	}

	private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		ois.defaultReadObject();
		this.registryClient = clientProvider.get();
	}

}
