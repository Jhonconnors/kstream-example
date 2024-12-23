package com.example.kstream.demo.context.impl;

import com.example.kstream.demo.config.KafkaDefinition;
import com.example.kstream.demo.context.ProcessContext;
import com.example.kstream.demo.context.TaskElement;
import com.example.kstream.demo.model.ClientInsight;
import com.example.kstream.demo.model.SmsNotification;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

import java.util.Map;


@Order(1)
@Component
public class TaskLogger extends TaskElement<ProcessContext<ClientInsight, SmsNotification>> {

    @Autowired
    private KafkaDefinition kafkaDefinition;
    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    @Override
    public void execute(ProcessContext<ClientInsight, SmsNotification> processContext) {

    }
}
