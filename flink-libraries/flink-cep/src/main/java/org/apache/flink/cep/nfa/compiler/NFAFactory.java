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

package org.apache.flink.cep.nfa.compiler;

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.base.EnumSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.State;
import org.apache.flink.cep.nfa.StateTransition;
import org.apache.flink.cep.nfa.StateTransitionAction;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * TODO Update
 * Implementation of the {@link NFAFactory} interface.
 *
 * <p>The implementation takes the input type serializer, the window time and the set of
 * states and their transitions to be able to create an NFA from them.
 *
 * @param <T> Type of the input events which are processed by the NFA
 */
public class NFAFactory<T> implements Serializable {

	private static final long serialVersionUID = 8939783698296714379L;

	private final long windowTime;
	private final Set<State<T>> states;
	private final boolean timeoutHandling;

	public NFAFactory(
		long windowTime,
		Set<State<T>> states,
		boolean timeoutHandling) {

		this.windowTime = windowTime;
		this.states = states;
		this.timeoutHandling = timeoutHandling;
	}

	public NFA<T> createNFA() {
		return new NFA<>(this);
	}

	public long getWindowTime() {
		return windowTime;
	}

	public Set<State<T>> getStates() {
		return Collections.unmodifiableSet(states);
	}

	public boolean isTimeoutHandling() {
		return timeoutHandling;
	}

	/**
	 * TODO Update.
	 * @param <T>
	 */
	public static class NFAFactorySerializerConfigSnapshot<T> extends TypeSerializerConfigSnapshot {

		private static final int VERSION = 1;

		@Override
		public int getVersion() {
			return VERSION;
		}

		@Override
		public boolean equals(Object obj) {
			return obj == this ||
				(obj != null && obj.getClass().equals(getClass()));
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(getClass());
		}
	}

	/**
	 * {@link TypeSerializer} for {@link NFAFactory}.
	 *
	 * @param <T> type of the input events
	 */
	public static class NFAFactorySerializer<T> extends TypeSerializer<NFAFactory<T>> {

		private static final long serialVersionUID = -6998974749963206458L;

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public TypeSerializer<NFAFactory<T>> duplicate() {
			return this;
		}

		@Override
		public NFAFactory<T> createInstance() {
			return null;
		}

		@Override
		public NFAFactory<T> copy(NFAFactory<T> from) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);

				serialize(from, new DataOutputViewStreamWrapper(oos));

				oos.close();
				baos.close();

				byte[] data = baos.toByteArray();

				ByteArrayInputStream bais = new ByteArrayInputStream(data);
				ObjectInputStream ois = new ObjectInputStream(bais);

				final NFAFactory<T> copy = deserialize(new DataInputViewStreamWrapper(ois));

				ois.close();
				bais.close();
				return copy;
			} catch (IOException e) {
				throw new RuntimeException("Could not copy NFAFactory.", e);
			}
		}

		@Override
		public NFAFactory<T> copy(NFAFactory<T> from, NFAFactory<T> reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(NFAFactory<T> record, DataOutputView target) throws IOException {
			serializeStates(record.states, target);
			target.writeLong(record.windowTime);
			target.writeBoolean(record.timeoutHandling);
		}

		@Override
		public NFAFactory<T> deserialize(DataInputView source) throws IOException {
			Set<State<T>> states = deserializeStates(source);
			long windowTime = source.readLong();
			boolean handleTimeout = source.readBoolean();

			return new NFAFactory<>(windowTime, states, handleTimeout);
		}

		@Override
		public NFAFactory<T> deserialize(NFAFactory<T> reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			Set<State<T>> states = deserializeStates(source);
			serializeStates(states, target);

			long windowTime = source.readLong();
			target.writeLong(windowTime);

			boolean handleTimeout = source.readBoolean();
			target.writeBoolean(handleTimeout);
		}

		@Override
		public boolean equals(Object obj) {
			return obj == this ||
				(obj != null && obj.getClass().equals(getClass()));
		}

		@Override
		public boolean canEqual(Object obj) {
			return true;
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(getClass());
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return new NFAFactorySerializerConfigSnapshot<>();
		}

		@Override
		public CompatibilityResult<NFAFactory<T>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			if (configSnapshot instanceof NFAFactorySerializerConfigSnapshot &&
				configSnapshot.getReadVersion() == NFAFactorySerializerConfigSnapshot.VERSION) {
				return CompatibilityResult.compatible();
			}
			return CompatibilityResult.requiresMigration();
		}

		private void serializeStates(Set<State<T>> states, DataOutputView out) throws IOException {
			TypeSerializer<String> nameSerializer = StringSerializer.INSTANCE;
			TypeSerializer<State.StateType> stateTypeSerializer = new EnumSerializer<>(State.StateType.class);
			TypeSerializer<StateTransitionAction> actionSerializer = new EnumSerializer<>(StateTransitionAction.class);

			out.writeInt(states.size());
			for (State<T> state : states) {
				nameSerializer.serialize(state.getName(), out);
				stateTypeSerializer.serialize(state.getStateType(), out);
			}

			for (State<T> state : states) {
				nameSerializer.serialize(state.getName(), out);

				out.writeInt(state.getStateTransitions().size());
				for (StateTransition<T> transition : state.getStateTransitions()) {
					nameSerializer.serialize(transition.getSourceState().getName(), out);
					nameSerializer.serialize(transition.getTargetState().getName(), out);
					actionSerializer.serialize(transition.getAction(), out);

					serializeCondition(transition.getCondition(), out);
				}
			}
		}

		private Set<State<T>> deserializeStates(DataInputView in) throws IOException {
			TypeSerializer<String> nameSerializer = StringSerializer.INSTANCE;
			TypeSerializer<State.StateType> stateTypeSerializer = new EnumSerializer<>(State.StateType.class);
			TypeSerializer<StateTransitionAction> actionSerializer = new EnumSerializer<>(StateTransitionAction.class);

			final int noOfStates = in.readInt();
			Map<String, State<T>> states = new HashMap<>(noOfStates);

			for (int i = 0; i < noOfStates; i++) {
				String stateName = nameSerializer.deserialize(in);
				State.StateType stateType = stateTypeSerializer.deserialize(in);

				State<T> state = new State<>(stateName, stateType);
				states.put(stateName, state);
			}

			for (int i = 0; i < noOfStates; i++) {
				String srcName = nameSerializer.deserialize(in);

				int noOfTransitions = in.readInt();
				for (int j = 0; j < noOfTransitions; j++) {
					String src = nameSerializer.deserialize(in);
					Preconditions.checkState(
						src.equals(srcName),
						"Source Edge names do not match (" + srcName + " - " + src + ").");

					String trgt = nameSerializer.deserialize(in);
					StateTransitionAction action = actionSerializer.deserialize(in);

					IterativeCondition<T> condition = null;
					try {
						condition = deserializeCondition(in);
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}

					State<T> srcState = states.get(src);
					State<T> trgtState = states.get(trgt);
					srcState.addStateTransition(action, trgtState, condition);
				}

			}
			return new HashSet<>(states.values());
		}

		private void serializeCondition(IterativeCondition<T> condition, DataOutputView out) throws IOException {
			out.writeBoolean(condition != null);
			if (condition != null) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);

				oos.writeObject(condition);

				oos.close();
				baos.close();

				byte[] serCondition = baos.toByteArray();
				out.writeInt(serCondition.length);
				out.write(serCondition);
			}
		}

		@SuppressWarnings("unchecked")
		private IterativeCondition<T> deserializeCondition(DataInputView in) throws IOException, ClassNotFoundException {
			boolean hasCondition = in.readBoolean();
			if (hasCondition) {
				int length = in.readInt();

				byte[] serCondition = new byte[length];
				in.read(serCondition);

				ByteArrayInputStream bais = new ByteArrayInputStream(serCondition);
				ObjectInputStream ois = new ObjectInputStream(bais);

				IterativeCondition<T> condition = (IterativeCondition<T>) ois.readObject();
				ois.close();
				bais.close();

				return condition;
			}
			return null;
		}
	}
}
