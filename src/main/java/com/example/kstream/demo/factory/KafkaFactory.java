package com.example.kstream.demo.factory;

import com.example.kstream.demo.config.KafkaDefinition;
import com.example.kstream.demo.model.TaskState;
import com.example.kstream.demo.model.TypeOperation;
import com.example.kstream.demo.context.ProcessContext;
import com.example.kstream.demo.model.SmsNotification;
import com.example.kstream.demo.model.ClientInsight;
import com.example.kstream.demo.service.StreamProcessingService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class KafkaFactory {

    private static final Logger logger = LoggerFactory.getLogger(KafkaFactory.class);

    @Autowired
    private KafkaDefinition kafkaDefinition;

    protected void customError(ProcessContext processContext) {
        logger.info("This is the Value : {}",processContext);
    }

    @Autowired
    private StreamProcessingService streamProcessingService;
    @Autowired
    private ProcessContext<ClientInsight, SmsNotification> process = new ProcessContext<>();

    @Autowired
    void buildPipeline(StreamsBuilder builder) {
        KStream<String, ProcessContext> stream = builder.stream(kafkaDefinition.getInputTopic(),
                        Consumed.with(Serdes.String(), new JsonSerde<>(ClientInsight.class)))
                .filter((key, value) -> value != null)
                .mapValues((value1) -> {
                    process.setInput(value1);
                    streamProcessingService.process(process);
                    return process;
                });
        stream.process(() -> new AbstractProcessor<String, ProcessContext>() {
            private KafkaConsumer<String, String> kafkaConsumer;
            @Override
            public void init(ProcessorContext context) {
                super.init(context);
            }
            @Override
            public void process(String key, ProcessContext value) {
                kafkaConsumer = createKafkaConsumer(this.context().applicationId());
                logginConsumerLag(kafkaConsumer, createListPartitions(kafkaConsumer));

                // Reenviar el mensaje al siguiente paso
                context().forward(key, value);
            }
            @Override
            public void close() {
                if (kafkaConsumer != null) {
                    kafkaConsumer.close();
                }
            }
        });

        splitStream(stream, kafkaDefinition.getOutputTopicAdvice(), kafkaDefinition.getRetryTopic(),
                kafkaDefinition.getErrorTopic(), ProcessContext.class, SmsNotification.class);
    }

    public <O, T extends ProcessContext<?, O>> void splitStream
            (KStream<String, T> stream, String oututTopicAdvice, String retryTopic,
             String errorTopic, Class<T> processContext,  Class<O> output) {
        stream.split()
                .branch((key, value) -> value.getTask().getState() == TaskState.Success,
                        Branched.withConsumer(ks -> ks
                                .peek((key, value) ->
                                        logger.info("Success Branch - Key: {}, Output: {}", key, value.getOutput()))
                                .mapValues(ProcessContext::getOutput)
                                .to(oututTopicAdvice, Produced.with(Serdes.String(), new JsonSerde<>(output)))))
                .branch((key, value) -> value.getTask().getState() == TaskState.Retry,
                        Branched.withConsumer(ks -> ks
                                .peek((key, value) -> logger.info("Retry Branch - Key: {}, Value: {}", key, value))
                                .to(retryTopic, Produced.with(Serdes.String(), new JsonSerde<>(processContext)))))
                .branch((key, value) -> value.getTask().getState() == TaskState.Error
                                && kafkaDefinition.getTypeOperation() ==TypeOperation.TOPIC,
                        Branched.withConsumer(ks -> ks
                                .peek((key, value) ->
                                        logger.info("Error Branch (TOPIC) - Key: {}, Value: {}", key, value))
                                .map((k, v) -> KeyValue.pair(v.getKey(), v))
                                .to(errorTopic, Produced.with(Serdes.String(), new JsonSerde<>(processContext)))))
                .branch((key, value) -> value.getTask().getState() == TaskState.Error
                                && kafkaDefinition.getTypeOperation() != TypeOperation.TOPIC,
                        Branched.withConsumer(ks -> ks
                                .mapValues(value ->{
                                    customError(value);
                                    return null;
                                })));
    }

    private List<TopicPartition> createListPartitions(KafkaConsumer<String, String> kafkaConsumer) {
        // Obtener las particiones del tópico
        List<TopicPartition> partitions = kafkaConsumer.partitionsFor(kafkaDefinition.getInputTopic())
                .stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .collect(Collectors.toList());
        kafkaConsumer.assign(partitions);
        return partitions;
    }

    private void logginConsumerLag(KafkaConsumer<String, String> kafkaConsumer, List<TopicPartition> partitions) {
        long totalLag = 0;

        for (TopicPartition partition : partitions) {
            // Obtener el offset comprometido
            long currentOffset = kafkaConsumer.position(partition);

            // Obtener el Log End Offset
            kafkaConsumer.seekToEnd(Collections.singletonList(partition));
            long logEndOffset = kafkaConsumer.position(partition);

            // Calcular el lag
            long lag = logEndOffset - currentOffset;
            totalLag += lag;

            logger.info("Partición: {}, Offset comprometido: {}, Log End Offset: {}, Lag: {}",
                    partition.partition(), currentOffset, logEndOffset, lag);
        }
        // Mostrar el lag total
        logger.info("Lag total del consumidor en todas las particiones: {}", totalLag);
    }

    private KafkaConsumer<String, String> createKafkaConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "extra_consumer");
        return new KafkaConsumer<>(props);
    }

}