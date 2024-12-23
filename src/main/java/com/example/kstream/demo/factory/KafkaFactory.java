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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Properties;

@Component
public class KafkaFactory {

    @Autowired
    private KafkaDefinition kafkaDefinition;

    protected void customError(ProcessContext processContext) {
        System.out.println("This is the Value : "+processContext);
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

        // Añadir un Processor personalizado para monitorear el lag
        stream.process(() -> new AbstractProcessor<String, ProcessContext>() {
            private KafkaConsumer<String, String> kafkaConsumer;

            @Override
            public void init(ProcessorContext context) {
                super.init(context);
//                // Configuración del consumidor Kafka adicional
                Properties props = new Properties();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "extra_consumer2");
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-dev-local-02");
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                kafkaConsumer = new KafkaConsumer<>(props);
            }

            @Override
            public void process(String key, ProcessContext value) {
                // Obtener el offset actual
                long currentOffset = this.context().offset();

                // Obtener el Log End Offset
                TopicPartition topicPartition = new TopicPartition(kafkaDefinition.getInputTopic(), this.context().partition());
                kafkaConsumer.assign(Collections.singletonList(topicPartition));
                kafkaConsumer.seekToEnd(Collections.singletonList(topicPartition));
                long logEndOffset = kafkaConsumer.position(topicPartition);

                // Calcular el lag
                long lag = logEndOffset - currentOffset;

                // Mostrar detalles
                System.out.printf(
                        "Offset actual: %d, Log End Offset: %d, Lag del consumidor: %d%n",
                        currentOffset, logEndOffset, lag
                );

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
                                .mapValues((context) -> {
                                    System.out.println("En el primer branch ...");
                                    return context.getOutput();
                                })
                                .to(oututTopicAdvice, Produced.with(Serdes.String(), new JsonSerde<>(output)))))
                .branch((key, value) -> value.getTask().getState() == TaskState.Retry,
                        Branched.withConsumer(ks -> ks
                                .to(retryTopic, Produced.with(Serdes.String(), new JsonSerde<>(processContext)))))
                .branch((key, value) -> value.getTask().getState() == TaskState.Error
                                && kafkaDefinition.getTypeOperation() ==TypeOperation.TOPIC,
                        Branched.withConsumer(ks -> ks
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



}