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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;

import java.io.IOException;

/**
 * Connecting channel that allows for waiting for the underlying connection to succeed.
 */
class ConnectingChannel implements ChannelFutureListener {

	private final Object connectLock = new Object();

	private final ConnectionID connectionId;

	private final NetworkClientHandler clientHandler;

	public ConnectingChannel(ConnectionID connectionId, NetworkClientHandler clientHandler) {
		this.connectionId = connectionId;
		this.clientHandler = clientHandler;
	}

	private void handInChannel(Channel channel) {
		synchronized (connectLock) {
			try {
				if (clientHandler != null) {
					channel.pipeline().replace(NetworkClientHandler.class, null, clientHandler);
				}

				this.channel = channel;
				connectLock.notifyAll();
			} catch (Throwable t) {
				notifyOfError(t);
			}
		}
	}

	private volatile Channel channel;

	private volatile Throwable error;

	Channel waitForChannel() throws IOException, InterruptedException {
		synchronized (connectLock) {
			while (error == null && channel == null) {
				connectLock.wait(2000);
			}
		}

		if (error != null) {
			throw new IOException("Connecting the channel failed: " + error.getMessage(), error);
		}

		return channel;
	}

	private void notifyOfError(Throwable error) {
		synchronized (connectLock) {
			this.error = error;
			connectLock.notifyAll();
		}
	}

	@Override
	public void operationComplete(ChannelFuture future) throws Exception {
		if (future.isSuccess()) {
			handInChannel(future.channel());
		} else if (future.cause() != null) {
			notifyOfError(new RemoteTransportException(
				"Connecting to remote task manager + '" + connectionId.getAddress() +
					"' has failed. This might indicate that the remote task " +
					"manager has been lost.",
				connectionId.getAddress(), future.cause()));
		} else {
			notifyOfError(new LocalTransportException(
				"Connecting to remote task manager + '" + connectionId.getAddress() +
					"' has been cancelled.", null));
		}
	}
}
