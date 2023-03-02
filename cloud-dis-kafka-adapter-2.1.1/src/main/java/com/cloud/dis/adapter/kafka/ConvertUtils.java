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
package com.cloud.dis.adapter.kafka;

import com.cloud.dis.adapter.common.consumer.DisConsumerRebalanceListener;
import com.cloud.dis.adapter.common.consumer.DisOffsetCommitCallback;
import com.cloud.dis.adapter.common.model.DisOffsetAndMetadata;
import com.cloud.dis.adapter.common.model.StreamPartition;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ConvertUtils {

    public static StreamPartition convert2StreamPartition(final TopicPartition topicPartition) {
        StreamPartition streamPartition = null;
        if (topicPartition != null) {
            streamPartition = new StreamPartition(topicPartition.topic(), topicPartition.partition());
        }
        return streamPartition;
    }

    public static TopicPartition convert2TopicPartition(final StreamPartition streamPartition) {
        TopicPartition topicPartition = null;
        if (streamPartition != null) {
            topicPartition = new TopicPartition(streamPartition.stream(), streamPartition.partition());
        }
        return topicPartition;
    }

    public static DisOffsetAndMetadata convert2DisOffsetAndMetadata(final OffsetAndMetadata offsetAndMetadata) {
        DisOffsetAndMetadata disOffsetAndMetadata = null;
        if (offsetAndMetadata != null) {
            disOffsetAndMetadata = new DisOffsetAndMetadata(offsetAndMetadata.offset(), offsetAndMetadata.metadata());
        }
        return disOffsetAndMetadata;
    }

    public static OffsetAndMetadata convert2OffsetAndMetadata(final DisOffsetAndMetadata disOffsetAndMetadata) {
        OffsetAndMetadata offsetAndMetadata = null;
        if (disOffsetAndMetadata != null) {
            offsetAndMetadata = new OffsetAndMetadata(disOffsetAndMetadata.offset(), disOffsetAndMetadata.metadata());
        }
        return offsetAndMetadata;
    }

    public static Map<StreamPartition, DisOffsetAndMetadata> convert2DisOffsetAndMetadataMap(final Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap) {
        Map<StreamPartition, DisOffsetAndMetadata> disOffsetAndMetadataMap = null;
        if (offsetAndMetadataMap != null) {
            disOffsetAndMetadataMap = new HashMap<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetAndMetadataMap.entrySet()) {
                disOffsetAndMetadataMap.put(convert2StreamPartition(entry.getKey()), convert2DisOffsetAndMetadata(entry.getValue()));
            }
        }
        return disOffsetAndMetadataMap;
    }

    public static Map<TopicPartition, OffsetAndMetadata> convert2OffsetAndMetadataMap(final Map<StreamPartition, DisOffsetAndMetadata> disOffsetAndMetadataMap) {
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = null;
        if (disOffsetAndMetadataMap != null) {
            offsetAndMetadataMap = new HashMap<>();
            for (Map.Entry<StreamPartition, DisOffsetAndMetadata> entry : disOffsetAndMetadataMap.entrySet()) {
                offsetAndMetadataMap.put(convert2TopicPartition(entry.getKey()), convert2OffsetAndMetadata(entry.getValue()));
            }
        }
        return offsetAndMetadataMap;
    }

    public static Set<TopicPartition> convert2TopicPartitionSet(final Set<StreamPartition> streamPartitionSet) {
        Set<TopicPartition> topicPartitionSet = null;
        if (streamPartitionSet != null) {
            topicPartitionSet = new HashSet<>();
            for (StreamPartition streamPartition : streamPartitionSet) {
                topicPartitionSet.add(convert2TopicPartition(streamPartition));
            }
        }
        return topicPartitionSet;
    }

    public static Set<StreamPartition> convert2StreamPartitionSet(final Set<TopicPartition> topicPartitionSet) {
        Set<StreamPartition> streamPartitionSet = null;
        if (topicPartitionSet != null) {
            streamPartitionSet = new HashSet<>();
            for (TopicPartition topicPartition : topicPartitionSet) {
                streamPartitionSet.add(convert2StreamPartition(topicPartition));
            }
        }
        return streamPartitionSet;
    }

    public static Collection<TopicPartition> convert2TopicPartitionCollection(final Collection<StreamPartition> streamPartitionCollection) {
        Collection<TopicPartition> topicPartitionCollection = null;
        if (streamPartitionCollection != null) {
            topicPartitionCollection = new HashSet<>();
            for (StreamPartition streamPartition : streamPartitionCollection) {
                topicPartitionCollection.add(convert2TopicPartition(streamPartition));
            }
        }
        return topicPartitionCollection;
    }

    public static Collection<StreamPartition> convert2StreamPartitionCollection(final Collection<TopicPartition> topicPartitionCollection) {
        Collection<StreamPartition> streamPartitionCollection = null;
        if (topicPartitionCollection != null) {
            streamPartitionCollection = new HashSet<>();
            for (TopicPartition topicPartition : topicPartitionCollection) {
                streamPartitionCollection.add(convert2StreamPartition(topicPartition));
            }
        }
        return streamPartitionCollection;
    }

    public static DisOffsetCommitCallback convert2DisOffsetCommitCallback(final OffsetCommitCallback offsetCommitCallback) {
        DisOffsetCommitCallback disOffsetCommitCallback = null;
        if (offsetCommitCallback != null) {
            disOffsetCommitCallback = new DisOffsetCommitCallback() {
                @Override
                public void onComplete(Map<StreamPartition, DisOffsetAndMetadata> offsets, Exception exception) {
                    offsetCommitCallback.onComplete(convert2OffsetAndMetadataMap(offsets), exception);
                }
            };
        }
        return disOffsetCommitCallback;
    }

    public static DisConsumerRebalanceListener convert2DisConsumerRebalanceListener(final ConsumerRebalanceListener consumerRebalanceListener) {
        DisConsumerRebalanceListener disConsumerRebalanceListener = null;
        if (consumerRebalanceListener != null) {
            disConsumerRebalanceListener = new DisConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<StreamPartition> streamPartitionCollection) {
                    consumerRebalanceListener.onPartitionsRevoked(convert2TopicPartitionCollection(streamPartitionCollection));

                }

                @Override
                public void onPartitionsAssigned(Collection<StreamPartition> streamPartitionCollection) {
                    consumerRebalanceListener.onPartitionsAssigned(convert2TopicPartitionCollection(streamPartitionCollection));
                }
            };
        }
        return disConsumerRebalanceListener;
    }

    public static Map<TopicPartition, Long> convert2TopicPartitionLongMap(final Map<StreamPartition, Long> streamPartitionLongMap) {
        Map<TopicPartition, Long> topicPartitionLongMap = null;
        if (streamPartitionLongMap != null) {
            topicPartitionLongMap = new HashMap<>();
            for (Map.Entry<StreamPartition, Long> entry : streamPartitionLongMap.entrySet()) {
                topicPartitionLongMap.put(convert2TopicPartition(entry.getKey()), entry.getValue());
            }
        }
        return topicPartitionLongMap;
    }

    public static Map<StreamPartition, Long> convert2StreamPartitionLongMap(final Map<TopicPartition, Long> topicPartitionLongMap) {
        Map<StreamPartition, Long> streamPartitionLongMap = null;
        if (topicPartitionLongMap != null) {
            streamPartitionLongMap = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> entry : topicPartitionLongMap.entrySet()) {
                streamPartitionLongMap.put(convert2StreamPartition(entry.getKey()), entry.getValue());
            }
        }
        return streamPartitionLongMap;
    }
}
