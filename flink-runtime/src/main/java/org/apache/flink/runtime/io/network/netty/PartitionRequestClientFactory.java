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
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Factory for {@link PartitionRequestClient} instances.
 * <p>
 * Instances of partition requests clients are shared among several {@link RemoteInputChannel}
 * instances.
 */
class PartitionRequestClientFactory {

	private final NettyClient nettyClient;

	private final ConcurrentMap<ConnectionID, Object> clients = new ConcurrentHashMap<ConnectionID, Object>();

	PartitionRequestClientFactory(NettyClient nettyClient) {
		this.nettyClient = nettyClient;
	}

	/**
	 * Atomically establishes a TCP connection to the given remote address and
	 * creates a {@link PartitionRequestClient} instance for this connection.
	 */
	PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId) throws IOException, InterruptedException {
		Object entry;
		PartitionRequestClient client = null;

		while (client == null) {
			entry = clients.get(connectionId);

			if (entry != null) {
				// Existing channel or connecting channel
				if (entry instanceof PartitionRequestClient) {
					client = (PartitionRequestClient) entry;
				}
				else {
					ConnectingChannel future = (ConnectingChannel) entry;
					client = getPartitionRequestClientForChannel(connectionId, future);
				}
			}
			else {
				// No channel yet. Create one, but watch out for a race.
				// We create a "connecting future" and atomically add it to the map.
				// Only the thread that really added it establishes the channel.
				// The others need to wait on that original establisher's future.
				ConnectingChannel connectingChannel = new ConnectingChannel(connectionId, null);
				Object old = clients.putIfAbsent(connectionId, connectingChannel);

				if (old == null) {
					nettyClient.connect(connectionId.getAddress()).addListener(connectingChannel);
					client = getPartitionRequestClientForChannel(connectionId, connectingChannel);
				}
				else if (old instanceof ConnectingChannel) {
					ConnectingChannel oldConnectingChannel = (ConnectingChannel) old;
					client = getPartitionRequestClientForChannel(connectionId, oldConnectingChannel);
				}
				else {
					client = (PartitionRequestClient) old;
				}
			}

			// Make sure to increment the reference count before handing a client
			// out to ensure correct bookkeeping for channel closing.
			if (!client.incrementReferenceCounter()) {
				destroyPartitionRequestClient(connectionId, client);
				client = null;
			}
		}

		return client;
	}

	private PartitionRequestClient getPartitionRequestClientForChannel(
			ConnectionID connectionId,
			ConnectingChannel connectingChannel) throws IOException, InterruptedException {

		Channel channel = connectingChannel.waitForChannel();
		clients.replace(connectionId, connectingChannel, new PartitionRequestClient(channel,
			channel.pipeline().get(NetworkClientHandler.class),
			connectionId,
			this));

		Object client = clients.get(connectionId);
		if (client instanceof PartitionRequestClient) {
			return (PartitionRequestClient) client;
		} else {
			throw new IllegalStateException(
				"Connecting channel was replaced with another instance of connecting channel, " +
					"which should never happen");
		}
	}

	public void closeOpenChannelConnections(ConnectionID connectionId) {
		Object entry = clients.get(connectionId);

		if (entry instanceof ConnectingChannel) {
			ConnectingChannel channel = (ConnectingChannel) entry;

			clients.remove(connectionId, channel);
		}
	}

	ConnectingChannel reconnect(Channel oldChannel, ConnectionID connectionID) {
		NetworkClientHandler networkClientHandler = oldChannel.pipeline().get(NetworkClientHandler.class);
		ConnectingChannel connectingChannel = new ConnectingChannel(connectionID, networkClientHandler);
		nettyClient.connect(connectionID.getAddress()).addListener(connectingChannel);
		return connectingChannel;
	}

	int getNumberOfActiveClients() {
		return clients.size();
	}

	/**
	 * Removes the client for the given {@link ConnectionID}.
	 */
	void destroyPartitionRequestClient(ConnectionID connectionId, PartitionRequestClient client) {
		clients.remove(connectionId, client);
	}

}
