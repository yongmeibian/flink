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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.api.common.time.Deadline;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Test for {@link RecordEmitter}. */
public class RecordEmitterTest {

	private static class TestRecordEmitter extends RecordEmitter<RecordWrapper<?>> {

		private List<RecordWrapper<?>> results = Collections.synchronizedList(new ArrayList<>());

		private TestRecordEmitter() {
			super(DEFAULT_QUEUE_CAPACITY);
		}

		@Override
		public void emit(Queue<RecordWrapper<?>> record, RecordQueue<RecordWrapper<?>> queue) {
			results.addAll(record);
			record.clear();
		}
	}

	@Test
	public void test() throws Exception {

		TestRecordEmitter emitter = new TestRecordEmitter();

		final RecordWrapper<String> one  = wrapRecord("one", 1);
		final RecordWrapper<String> two  = wrapRecord("two", 2);
		final RecordWrapper<String> five = wrapRecord("five", 5);
		final RecordWrapper<String> ten  = wrapRecord("ten", 10);

		final RecordEmitter.RecordQueue<RecordWrapper<?>> queue0 = emitter.getQueue(0);
		final RecordEmitter.RecordQueue<RecordWrapper<?>> queue1 = emitter.getQueue(1);

		queue0.put(one);
		queue0.put(five);
		queue0.put(ten);

		queue1.put(two);

		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.submit(emitter);

		Deadline dl = Deadline.fromNow(Duration.ofSeconds(10));
		while (emitter.results.size() != 4 && dl.hasTimeLeft()) {
			Thread.sleep(10);
		}
		emitter.stop();
		executor.shutdownNow();

		Assert.assertThat(emitter.results, Matchers.contains(one, five, two, ten));
	}

	@Test
	public void testRetainMinAfterReachingLimit() throws Exception {

		TestRecordEmitter emitter = new TestRecordEmitter();

		final RecordWrapper<String> one    = wrapRecord("1", 1);
		final RecordWrapper<String> two    = wrapRecord("2", 2);
		final RecordWrapper<String> three  = wrapRecord("3", 3);
		final RecordWrapper<String> ten    = wrapRecord("10", 10);
		final RecordWrapper<String> eleven = wrapRecord("11", 11);

		final RecordWrapper<String> twenty = wrapRecord("20", 20);
		final RecordWrapper<String> thirty = wrapRecord("30", 30);

		final RecordEmitter.RecordQueue<RecordWrapper<?>> queue0 = emitter.getQueue(0);
		final RecordEmitter.RecordQueue<RecordWrapper<?>> queue1 = emitter.getQueue(1);

		queue0.put(one);
		queue0.put(two);
		queue0.put(three);
		queue0.put(ten);
		queue0.put(eleven);

		queue1.put(twenty);
		queue1.put(thirty);

		emitter.setMaxLookaheadMillis(1);
		emitter.setCurrentWatermark(5);

		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.submit(emitter);
		try {
			// emits one record past the limit
			Deadline dl = Deadline.fromNow(Duration.ofSeconds(10));
			while (emitter.results.size() != 4 && dl.hasTimeLeft()) {
				Thread.sleep(10);
			}
			Assert.assertThat(emitter.results, Matchers.contains(one, two, three, ten));

			// advance watermark, emits remaining record from queue0
			emitter.setCurrentWatermark(10);
			dl = Deadline.fromNow(Duration.ofSeconds(10));
			while (emitter.results.size() != 5 && dl.hasTimeLeft()) {
				Thread.sleep(10);
			}
			Assert.assertThat(emitter.results, Matchers.contains(one, two, three, ten, eleven));
		}
		finally {
			emitter.stop();
			executor.shutdownNow();
		}
	}

	private static RecordWrapper<String> wrapRecord(String record, long timestamp) {
		return new RecordWrapper<>(
			record,
			timestamp,
			0,
			null,
			null,
			true
		);
	}
}
