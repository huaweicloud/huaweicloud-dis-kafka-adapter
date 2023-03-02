/*
 * Copyright 2014-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.EmbeddedHeaderUtils;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.MessageValues;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.Lifecycle;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.integration.kafka.support.RawRecordHeaderErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.cloud.streamkafka.disadapter.DISKafkaConsumerFactory;
import com.cloud.streamkafka.disadapter.DISKafkaProducerFactory;

/*
 * 在原代码基础上，只是把KafkaProducerFactory和KafkaConsumerFactory替换为DISKafkaProducerFactory和DISKafkaConsumerFactory
 * */

/**
 * A {@link Binder} that uses Kafka as the underlying middleware.
 *
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 * @author Gary Russell
 * @author Mark Fisher
 * @author Soby Chacko
 * @author Henryk Konsek
 * @author Doug Saus
 * @author Aldo Sinanaj
 */
public class KafkaMessageChannelBinder extends
        AbstractMessageChannelBinder<ExtendedConsumerProperties<KafkaConsumerProperties>,
            ExtendedProducerProperties<KafkaProducerProperties>, KafkaTopicProvisioner>
        implements ExtendedPropertiesBinder<MessageChannel, KafkaConsumerProperties, KafkaProducerProperties> {

    public static final String X_ORIGINAL_TOPIC = "x-original-topic";

    public static final String X_EXCEPTION_MESSAGE = "x-exception-message";

    public static final String X_EXCEPTION_STACKTRACE = "x-exception-stacktrace";

    private final KafkaBinderConfigurationProperties configurationProperties;

    private final Map<String, TopicInformation> topicsInUse = new HashMap<>();

    private ProducerListener<byte[], byte[]> producerListener;

    private KafkaExtendedBindingProperties extendedBindingProperties = new KafkaExtendedBindingProperties();

    public KafkaMessageChannelBinder(KafkaBinderConfigurationProperties configurationProperties,
            KafkaTopicProvisioner provisioningProvider) {
        super(false, headersToMap(configurationProperties), provisioningProvider);
        this.configurationProperties = configurationProperties;
    }

    private static String[] headersToMap(KafkaBinderConfigurationProperties configurationProperties) {
        String[] headersToMap;
        if (ObjectUtils.isEmpty(configurationProperties.getHeaders())) {
            headersToMap = BinderHeaders.STANDARD_HEADERS;
        }
        else {
            String[] combinedHeadersToMap = Arrays.copyOfRange(BinderHeaders.STANDARD_HEADERS, 0,
                    BinderHeaders.STANDARD_HEADERS.length + configurationProperties.getHeaders().length);
            System.arraycopy(configurationProperties.getHeaders(), 0, combinedHeadersToMap,
                    BinderHeaders.STANDARD_HEADERS.length,
                    configurationProperties.getHeaders().length);
            headersToMap = combinedHeadersToMap;
        }
        return headersToMap;
    }

    public void setExtendedBindingProperties(KafkaExtendedBindingProperties extendedBindingProperties) {
        this.extendedBindingProperties = extendedBindingProperties;
    }

    public void setProducerListener(ProducerListener<byte[], byte[]> producerListener) {
        this.producerListener = producerListener;
    }

    Map<String, TopicInformation> getTopicsInUse() {
        return this.topicsInUse;
    }

    @Override
    public KafkaConsumerProperties getExtendedConsumerProperties(String channelName) {
        return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
    }

    @Override
    public KafkaProducerProperties getExtendedProducerProperties(String channelName) {
        return this.extendedBindingProperties.getExtendedProducerProperties(channelName);
    }

    @Override
    protected MessageHandler createProducerMessageHandler(final ProducerDestination destination,
            ExtendedProducerProperties<KafkaProducerProperties> producerProperties, MessageChannel errorChannel)
                    throws Exception {
        final DISKafkaProducerFactory<byte[], byte[]> producerFB = getProducerFactory(producerProperties);
        Collection<PartitionInfo> partitions = provisioningProvider.getPartitionsForTopic(
                producerProperties.getPartitionCount(),
                false,
                new Callable<Collection<PartitionInfo>>() {

                    @Override
                    public Collection<PartitionInfo> call() throws Exception {
                        Producer<byte[], byte[]> producer = producerFB.createProducer();
                        List<PartitionInfo> partitionsFor = producer.partitionsFor(destination.getName());
                        producer.close();
                        producerFB.destroy();
                        return partitionsFor;
                    }

                });
        this.topicsInUse.put(destination.getName(), new TopicInformation(null, partitions));
        if (producerProperties.getPartitionCount() < partitions.size()) {
            if (this.logger.isInfoEnabled()) {
                this.logger.info("The `partitionCount` of the producer for topic " + destination.getName() + " is "
                        + producerProperties.getPartitionCount() + ", smaller than the actual partition count of "
                        + partitions.size() + " of the topic. The larger number will be used instead.");
            }
            /*
             * This is dirty; it relies on the fact that we, and the partition
             * interceptor, share a hard reference to the producer properties instance.
             * But I don't see another way to fix it since the interceptor has already
             * been added to the channel, and we don't have access to the channel here; if
             * we did, we could inject the proper partition count there.
             * TODO: Consider this when doing the 2.0 binder restructuring.
             */
            producerProperties.setPartitionCount(partitions.size());
        }

        KafkaTemplate<byte[], byte[]> kafkaTemplate = new KafkaTemplate<>(producerFB);
        if (this.producerListener != null) {
            kafkaTemplate.setProducerListener(this.producerListener);
        }
        ProducerConfigurationMessageHandler handler = new ProducerConfigurationMessageHandler(kafkaTemplate,
                destination.getName(), producerProperties, producerFB);
        if (errorChannel != null) {
            handler.setSendFailureChannel(errorChannel);
        }
        return handler;
    }

    private DISKafkaProducerFactory<byte[], byte[]> getProducerFactory(
            ExtendedProducerProperties<KafkaProducerProperties> producerProperties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, String.valueOf(this.configurationProperties.getRequiredAcks()));
        if (!ObjectUtils.isEmpty(configurationProperties.getProducerConfiguration())) {
            props.putAll(configurationProperties.getProducerConfiguration());
        }
        if (ObjectUtils.isEmpty(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.configurationProperties.getKafkaConnectionString());
        }
        if (ObjectUtils.isEmpty(props.get(ProducerConfig.BATCH_SIZE_CONFIG))) {
            props.put(ProducerConfig.BATCH_SIZE_CONFIG,
                    String.valueOf(producerProperties.getExtension().getBufferSize()));
        }
        if (ObjectUtils.isEmpty(props.get(ProducerConfig.LINGER_MS_CONFIG))) {
            props.put(ProducerConfig.LINGER_MS_CONFIG,
                    String.valueOf(producerProperties.getExtension().getBatchTimeout()));
        }
        if (ObjectUtils.isEmpty(props.get(ProducerConfig.COMPRESSION_TYPE_CONFIG))) {
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                    producerProperties.getExtension().getCompressionType().toString());
        }
        if (!ObjectUtils.isEmpty(producerProperties.getExtension().getConfiguration())) {
            props.putAll(producerProperties.getExtension().getConfiguration());
        }
        return new DISKafkaProducerFactory<>(props);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected MessageProducer createConsumerEndpoint(final ConsumerDestination destination, final String group,
            final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {

        boolean anonymous = !StringUtils.hasText(group);
        Assert.isTrue(!anonymous || !extendedConsumerProperties.getExtension().isEnableDlq(),
                "DLQ support is not available for anonymous subscriptions");
        String consumerGroup = anonymous ? "anonymous." + UUID.randomUUID().toString() : group;
        final ConsumerFactory<?, ?> consumerFactory = createKafkaConsumerFactory(anonymous, consumerGroup,
                extendedConsumerProperties);
        int partitionCount = extendedConsumerProperties.getInstanceCount()
                * extendedConsumerProperties.getConcurrency();

        Collection<PartitionInfo> allPartitions = provisioningProvider.getPartitionsForTopic(partitionCount,
                extendedConsumerProperties.getExtension().isAutoRebalanceEnabled(),
                new Callable<Collection<PartitionInfo>>() {

                    @Override
                    public Collection<PartitionInfo> call() throws Exception {
                        Consumer<?, ?> consumer = consumerFactory.createConsumer();
                        List<PartitionInfo> partitionsFor = consumer.partitionsFor(destination.getName());
                        consumer.close();
                        return partitionsFor;
                    }

                });

        Collection<PartitionInfo> listenedPartitions;

        if (extendedConsumerProperties.getExtension().isAutoRebalanceEnabled() ||
                extendedConsumerProperties.getInstanceCount() == 1) {
            listenedPartitions = allPartitions;
        }
        else {
            listenedPartitions = new ArrayList<>();
            for (PartitionInfo partition : allPartitions) {
                // divide partitions across modules
                if ((partition.partition()
                        % extendedConsumerProperties.getInstanceCount()) == extendedConsumerProperties
                                .getInstanceIndex()) {
                    listenedPartitions.add(partition);
                }
            }
        }
        this.topicsInUse.put(destination.getName(), new TopicInformation(group, listenedPartitions));

        Assert.isTrue(!CollectionUtils.isEmpty(listenedPartitions), "A list of partitions must be provided");
        final TopicPartitionInitialOffset[] topicPartitionInitialOffsets = getTopicPartitionInitialOffsets(
                listenedPartitions);
        final ContainerProperties containerProperties = anonymous
                || extendedConsumerProperties.getExtension().isAutoRebalanceEnabled()
                        ? new ContainerProperties(destination.getName())
                        : new ContainerProperties(topicPartitionInitialOffsets);
        int concurrency = Math.min(extendedConsumerProperties.getConcurrency(), listenedPartitions.size());
        @SuppressWarnings("rawtypes")
        final ConcurrentMessageListenerContainer<?, ?> messageListenerContainer =
                new ConcurrentMessageListenerContainer(
                        consumerFactory, containerProperties) {

            @Override
            public void stop(Runnable callback) {
                super.stop(callback);
            }

        };
        messageListenerContainer.setConcurrency(concurrency);
        if (!extendedConsumerProperties.getExtension().isAutoCommitOffset()) {
            messageListenerContainer.getContainerProperties()
                    .setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);
            messageListenerContainer.getContainerProperties().setAckOnError(false);
        }
        else {
            messageListenerContainer.getContainerProperties()
                    .setAckOnError(isAutoCommitOnError(extendedConsumerProperties));
        }
        if (this.logger.isDebugEnabled()) {
            this.logger.debug(
                    "Listened partitions: " + StringUtils.collectionToCommaDelimitedString(listenedPartitions));
        }
        final KafkaMessageDrivenChannelAdapter<?, ?> kafkaMessageDrivenChannelAdapter = new KafkaMessageDrivenChannelAdapter<>(
                messageListenerContainer);
        kafkaMessageDrivenChannelAdapter.setBeanFactory(this.getBeanFactory());
        ErrorInfrastructure errorInfrastructure = registerErrorInfrastructure(destination, consumerGroup,
                extendedConsumerProperties);
        if (extendedConsumerProperties.getMaxAttempts() > 1) {
            kafkaMessageDrivenChannelAdapter.setRetryTemplate(buildRetryTemplate(extendedConsumerProperties));
            kafkaMessageDrivenChannelAdapter.setRecoveryCallback(errorInfrastructure.getRecoverer());
        }
        else {
            kafkaMessageDrivenChannelAdapter.setErrorChannel(errorInfrastructure.getErrorChannel());
        }
        return kafkaMessageDrivenChannelAdapter;
    }

    @Override
    protected ErrorMessageStrategy getErrorMessageStrategy() {
        return new RawRecordHeaderErrorMessageStrategy();
    }

    @Override
    protected MessageHandler getErrorMessageHandler(final ConsumerDestination destination, final String group,
            final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {
        if (extendedConsumerProperties.getExtension().isEnableDlq()) {
            DISKafkaProducerFactory<byte[], byte[]> producerFactory = getProducerFactory(
                    new ExtendedProducerProperties<>(new KafkaProducerProperties()));
            final KafkaTemplate<byte[], byte[]> kafkaTemplate = new KafkaTemplate<>(producerFactory);
            return new MessageHandler() {

                @Override
                public void handleMessage(Message<?> message) throws MessagingException {
                    final ConsumerRecord<?, ?> record = message.getHeaders()
                            .get(KafkaMessageDrivenChannelAdapter.KAFKA_RAW_DATA, ConsumerRecord.class);
                    final byte[] key = record.key() != null
                            ? Utils.toArray(ByteBuffer.wrap((byte[]) record.key()))
                            : null;
                    final byte[] payload;
                    if (message.getPayload() instanceof Throwable) {
                        final Throwable throwable = (Throwable) message.getPayload();
                        final String failureMessage = throwable.getMessage();

                        try {
                            MessageValues messageValues = EmbeddedHeaderUtils
                                    .extractHeaders(MessageBuilder.withPayload((byte[]) record.value()).build(),
                                            false);
                            messageValues.put(X_ORIGINAL_TOPIC, record.topic());
                            messageValues.put(X_EXCEPTION_MESSAGE, failureMessage);
                            messageValues.put(X_EXCEPTION_STACKTRACE, getStackTraceAsString(throwable));

                            final String[] headersToEmbed = new ArrayList<>(messageValues.keySet()).toArray(
                                    new String[messageValues.keySet().size()]);
                            payload = EmbeddedHeaderUtils.embedHeaders(messageValues,
                                    EmbeddedHeaderUtils.headersToEmbed(headersToEmbed));
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    else {
                        payload = record.value() != null
                                ? Utils.toArray(ByteBuffer.wrap((byte[]) record.value())) : null;
                    }
                    String dlqName = StringUtils.hasText(extendedConsumerProperties.getExtension().getDlqName())
                            ? extendedConsumerProperties.getExtension().getDlqName()
                            : "error." + destination.getName() + "." + group;
                    ListenableFuture<SendResult<byte[], byte[]>> sentDlq = kafkaTemplate.send(dlqName,
                            record.partition(), key, payload);
                    sentDlq.addCallback(new ListenableFutureCallback<SendResult<byte[], byte[]>>() {
                        StringBuilder sb = new StringBuilder().append(" a message with key='")
                                .append(toDisplayString(ObjectUtils.nullSafeToString(key), 50)).append("'")
                                .append(" and payload='")
                                .append(toDisplayString(ObjectUtils.nullSafeToString(payload), 50))
                                .append("'").append(" received from ")
                                .append(record.partition());

                        @Override
                        public void onFailure(Throwable ex) {
                            KafkaMessageChannelBinder.this.logger.error(
                                    "Error sending to DLQ " + sb.toString(), ex);
                        }

                        @Override
                        public void onSuccess(SendResult<byte[], byte[]> result) {
                            if (KafkaMessageChannelBinder.this.logger.isDebugEnabled()) {
                                KafkaMessageChannelBinder.this.logger.debug(
                                        "Sent to DLQ " + sb.toString());
                            }
                        }

                    });
                }
            };
        }
        return null;
    }

    private ConsumerFactory<?, ?> createKafkaConsumerFactory(boolean anonymous, String consumerGroup,
            ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, anonymous ? "latest" : "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

        if (!ObjectUtils.isEmpty(configurationProperties.getConsumerConfiguration())) {
            props.putAll(configurationProperties.getConsumerConfiguration());
        }
        if (ObjectUtils.isEmpty(props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.configurationProperties.getKafkaConnectionString());
        }
        if (!ObjectUtils.isEmpty(consumerProperties.getExtension().getConfiguration())) {
            props.putAll(consumerProperties.getExtension().getConfiguration());
        }
        if (!ObjectUtils.isEmpty(consumerProperties.getExtension().getStartOffset())) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                    consumerProperties.getExtension().getStartOffset().name());
        }

        return new DISKafkaConsumerFactory<>(props);
    }

    private boolean isAutoCommitOnError(ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
        return properties.getExtension().getAutoCommitOnError() != null
                ? properties.getExtension().getAutoCommitOnError()
                : properties.getExtension().isAutoCommitOffset() && properties.getExtension().isEnableDlq();
    }

    private TopicPartitionInitialOffset[] getTopicPartitionInitialOffsets(
            Collection<PartitionInfo> listenedPartitions) {
        final TopicPartitionInitialOffset[] topicPartitionInitialOffsets = new TopicPartitionInitialOffset[listenedPartitions
                .size()];
        int i = 0;
        for (PartitionInfo partition : listenedPartitions) {

            topicPartitionInitialOffsets[i++] = new TopicPartitionInitialOffset(partition.topic(),
                    partition.partition());
        }
        return topicPartitionInitialOffsets;
    }

    private String toDisplayString(String original, int maxCharacters) {
        if (original.length() <= maxCharacters) {
            return original;
        }
        return original.substring(0, maxCharacters) + "...";
    }

    private String getStackTraceAsString(Throwable cause) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter, true);
        cause.printStackTrace(printWriter);
        return stringWriter.getBuffer().toString();
    }

    private final class ProducerConfigurationMessageHandler extends KafkaProducerMessageHandler<byte[], byte[]>
            implements Lifecycle {

        private boolean running = true;

        private final DISKafkaProducerFactory<byte[], byte[]> producerFactory;

        ProducerConfigurationMessageHandler(KafkaTemplate<byte[], byte[]> kafkaTemplate, String topic,
                ExtendedProducerProperties<KafkaProducerProperties> producerProperties,
                DISKafkaProducerFactory<byte[], byte[]> producerFactory) {
            super(kafkaTemplate);
            setTopicExpression(new LiteralExpression(topic));
            setMessageKeyExpression(producerProperties.getExtension().getMessageKeyExpression());
            setBeanFactory(KafkaMessageChannelBinder.this.getBeanFactory());
            if (producerProperties.isPartitioned()) {
                SpelExpressionParser parser = new SpelExpressionParser();
                setPartitionIdExpression(parser.parseExpression("headers." + BinderHeaders.PARTITION_HEADER));
            }
            if (producerProperties.getExtension().isSync()) {
                setSync(true);
            }
            this.producerFactory = producerFactory;
        }

        @Override
        public void start() {
            try {
                super.onInit();
            }
            catch (Exception e) {
                this.logger.error("Initialization errors: ", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void stop() {
            producerFactory.stop();
            this.running = false;
        }

        @Override
        public boolean isRunning() {
            return this.running;
        }
    }

    public static class TopicInformation {

        private final String consumerGroup;

        private final Collection<PartitionInfo> partitionInfos;

        public TopicInformation(String consumerGroup, Collection<PartitionInfo> partitionInfos) {
            this.consumerGroup = consumerGroup;
            this.partitionInfos = partitionInfos;
        }

        public String getConsumerGroup() {
            return consumerGroup;
        }

        public boolean isConsumerTopic() {
            return consumerGroup != null;
        }

        public Collection<PartitionInfo> getPartitionInfos() {
            return partitionInfos;
        }

    }

}
