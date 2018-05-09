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

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.ConsumerRecordMetaInfo;
import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

/**
 * Base class for deserialization schema for Avro records. It takes care of managing lifecycle of
 * readers and provides common way of extracting reader schema.
 */
public abstract class AbstractAvroDeserializationSchema<T>
	extends AbstractDeserializationSchema<T> {

	/**
	 * Class to deserialize to.
	 */
	private Class<T> recordClazz;

	/**
	 * Reader that deserializes byte array into a record.
	 */
	private transient GenericDatumReader<T> datumReader;

	/**
	 * Input stream to read message from.
	 */
	private transient MutableByteArrayInputStream inputStream;

	/**
	 * Avro decoder that decodes binary data.
	 */
	private transient Decoder decoder;

	/**
	 * Avro schema for the reader.
	 */
	private transient Schema reader;

	/**
	 * Creates a Avro deserialization schema.
	 *
	 * @param recordClazz class to which deserialize. Should be one of:
	 *                    {@link org.apache.avro.specific.SpecificRecord},
	 *                    {@link org.apache.avro.generic.GenericRecord} or a POJO.
	 * @param reader      reader's Avro schema. Should be provided if recordClazz is
	 *                    {@link GenericRecord}
	 */
	protected AbstractAvroDeserializationSchema(Class<T> recordClazz, @Nullable Schema reader) {
		super(recordClazz);
		Preconditions.checkNotNull(recordClazz, "Avro record class must not be null.");
		this.recordClazz = recordClazz;
		this.inputStream = new MutableByteArrayInputStream();
		this.decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
		this.reader = reader;
	}

	protected GenericDatumReader<T> getDatumReader() {
		if (datumReader != null) {
			return datumReader;
		}

		if (SpecificRecord.class.isAssignableFrom(recordClazz)) {
			this.datumReader = new SpecificDatumReader<>();
		} else if (GenericRecord.class.isAssignableFrom(recordClazz)) {
			this.datumReader = new GenericDatumReader<>();
		} else {
			this.datumReader = new ReflectDatumReader<>();
		}

		return datumReader;
	}

	protected Schema getReaderSchema() {
		if (reader != null) {
			return reader;
		}

		if (SpecificRecord.class.isAssignableFrom(recordClazz)) {
			this.reader = SpecificData.get().getSchema(recordClazz);
		} else if (GenericRecord.class.isAssignableFrom(recordClazz)) {
			throw new IllegalStateException(
				"Cannot infer schema for generic record. Please pass explicit schema in the ctor.");
		} else {
			this.reader = ReflectData.get().getSchema(recordClazz);
		}

		return reader;
	}

	protected Decoder getDecoder() {
		return decoder;
	}

	@Override
	public T deserialize(byte[] message) {
		throw new UnsupportedOperationException("This method is no longer supported!");
	}

	protected abstract T doDeserialize(
		InputStream inputStream,
		ConsumerRecordMetaInfo consumerRecordMetaInfo) throws Exception;

	@Override
	public T deserialize(ConsumerRecordMetaInfo consumerRecordMetaInfossage) {
		// read record
		try {
			inputStream.setBuffer(consumerRecordMetaInfossage.getMessage());
			return doDeserialize(inputStream, consumerRecordMetaInfossage);
		} catch (Exception e) {
			throw new RuntimeException("Failed to deserialize Row.", e);
		}
	}

	private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		ois.defaultReadObject();
		this.inputStream = new MutableByteArrayInputStream();
		this.decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
	}

	/**
	 * An extension of the ByteArrayInputStream that allows to change a buffer that should be
	 * read without creating a new ByteArrayInputStream instance. This allows to re-use the same
	 * InputStream instance, copying message to process, and creation of Decoder on every new message.
	 */
	private static final class MutableByteArrayInputStream extends ByteArrayInputStream {

		MutableByteArrayInputStream() {
			super(new byte[0]);
		}

		/**
		 * Set buffer that can be read via the InputStream interface and reset the input stream.
		 * This has the same effect as creating a new ByteArrayInputStream with a new buffer.
		 *
		 * @param buf the new buffer to read.
		 */
		void setBuffer(byte[] buf) {
			this.buf = buf;
			this.pos = 0;
			this.count = buf.length;
		}
	}
}
