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

import org.apache.flink.api.common.serialization.ConsumerRecordMetaInfo;
import org.apache.flink.api.common.serialization.DeserializationSchema;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AbstractAvroDeserializationSchema}.
 */
public class AbstractAvroRowDeserializationSchemaTest {

	public static class SimplePojo {
		private String name;
		private int age;

		public SimplePojo() {
		}

		public SimplePojo(String name, int age) {
			this.name = name;
			this.age = age;
		}
	}

	@Test
	public void testReflectDataSerialization() throws IOException {
		DeserializationSchema<SimplePojo> deserializationSchema = AvroSerDe.deserialize(
			SimplePojo.class).build();

		ConsumerRecordMetaInfo record = mock(ConsumerRecordMetaInfo.class);
		when(record.getMessage()).then((Answer<byte[]>) invocationOnMock -> {
			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
			new ReflectDatumWriter<>(SimplePojo.class)
				.write(
					new SimplePojo("Anna", 25),
					encoder
				);

			encoder.flush();
			return stream.toByteArray();
		});

		SimplePojo anna = deserializationSchema.deserialize(record);
		assertEquals("Anna", anna.name);
		assertEquals(25, anna.age);
	}
}
