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

package org.apache.flink.streaming.connectors.kafka.serializers;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;

public abstract class AbstractAvroSerializationSchema<T>
	implements SerializationSchema<T> {

	/**
	 * Avro record class.
	 */
	private Class<T> recordClazz;

	/**
	 * Writer to serialize Avro record into a byte array.
	 */
	private transient GenericDatumWriter<T> datumWriter;

	/**
	 * Output stream to serialize records into byte array.
	 */
	private transient ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();

	/**
	 * Low-level class for serialization of Avro values.
	 */
	private transient Encoder encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);

	private transient Schema schema;

	/**
	 * Creates a Avro serialization schema for the given schema.
	 *
	 * @param recordClazz 
	 */
	protected AbstractAvroSerializationSchema(Class<T> recordClazz) {
		Preconditions.checkNotNull(recordClazz, "Avro record class must not be null.");
		this.recordClazz = recordClazz;
	}

	@Override
	public byte[] serialize(T record) {
		// convert to record

		// write
		try {
			arrayOutputStream.reset();
			doSerialize(arrayOutputStream, record);
			encoder.flush();
			return arrayOutputStream.toByteArray();
		} catch (Exception e) {
			throw new RuntimeException("Failed to serialize Row.", e);
		}
	}

	protected abstract void doSerialize(
		OutputStream out,
		T record) throws Exception;


	private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		ois.defaultReadObject();
		this.arrayOutputStream = new ByteArrayOutputStream();
		this.encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);
	}

	protected Schema getSchema() {
		if (schema != null) {
			return schema;
		}

		if (SpecificRecord.class.isAssignableFrom(recordClazz)) {
			this.schema = SpecificData.get().getSchema(recordClazz);
		} else if (GenericRecord.class.isAssignableFrom(recordClazz)) {
			throw new IllegalStateException(
				"Cannot infer schema for generic record. Please pass explicit schema in the ctor.");
		} else {
			this.schema = ReflectData.get().getSchema(recordClazz);
		}

		return schema;
	}

	protected Encoder getEncoder() {
		return encoder;
	}

	protected GenericDatumWriter<T> getDatumWriter() {
		if (datumWriter != null) {
			return datumWriter;
		}

		if (SpecificRecord.class.isAssignableFrom(recordClazz)) {
			this.datumWriter = new SpecificDatumWriter<>();
		} else if (GenericRecord.class.isAssignableFrom(recordClazz)) {
			this.datumWriter = new GenericDatumWriter<>();
		} else {
			this.datumWriter = new ReflectDatumWriter<>();
		}

		return datumWriter;
	}

}
