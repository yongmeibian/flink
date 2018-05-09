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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;

import java.io.IOException;
import java.io.OutputStream;

public class AvroSerializationSchema<T> extends AbstractAvroSerializationSchema<T> {
	/**
	 * Creates a Avro serialization schema for the given schema.
	 *
	 * @param recordClazz Avro record class used to deserialize Avro's record to Flink's row
	 */
	AvroSerializationSchema(Class<T> recordClazz) {
		super(recordClazz);
	}

	@Override
	protected void doSerialize(
		OutputStream out,
		T record) throws IOException {
		Schema schema = getSchema();

		GenericDatumWriter<T> datumWriter = getDatumWriter();
		datumWriter.setSchema(schema);
		datumWriter.write(record, getEncoder());
	}
}
