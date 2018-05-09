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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.serializers.AvroSerDe;
import org.apache.flink.streaming.connectors.kafka.serializers.generated.Address;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import static org.apache.flink.streaming.connectors.kafka.serializers.AvroSerDe.confluentSchemaRegistry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ConfluentSchemaRegistryAvroDeserializationSchema}.
 */
public class ConfluentSchemaRegistryAvroDeserializationSchemaTest {

	@Test
	public void test() throws Exception {
		MockSchemaRegistryClient client = new MockSchemaRegistryClient();
		DeserializationSchema<GenericRecord> deserializer =
			AvroSerDe.deserialize(GenericRecord.class)
				.forSchema(Address.SCHEMA$)
				.usingSchemaRegistry(
					confluentSchemaRegistry()
						.customRegistryClient(() -> client))
				.build();

		int schemaId = client.register("testTopic", Address.getClassSchema());

		ConsumerRecordMetaInfo record = mock(ConsumerRecordMetaInfo.class);
		when(record.getTopic()).thenReturn("testTopic");
		when(record.getMessage()).then((Answer<byte[]>) invocationOnMock -> {
			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			DataOutputStream dataOutputStream = new DataOutputStream(stream);

			dataOutputStream.writeByte(0);
			dataOutputStream.writeInt(schemaId);

			BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
			new SpecificDatumWriter<>(Address.class)
				.write(
					Address.newBuilder().setNum(1).setStreet("street")
						.setCity("city").setState("State").setZip("zip")
						.build(),
					encoder
				);

			encoder.flush();
			return stream.toByteArray();
		});

		deserializer.deserialize(record);
	}
}
