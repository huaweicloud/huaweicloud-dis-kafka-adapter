/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloud.dis.adapter.kafka.clients.consumer.internals;

import com.cloud.dis.adapter.kafka.clients.consumer.ConsumerRebalanceListener;
import com.cloud.dis.adapter.kafka.common.TopicPartition;

import java.util.Collection;

public class NoOpConsumerRebalanceListener implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

}
