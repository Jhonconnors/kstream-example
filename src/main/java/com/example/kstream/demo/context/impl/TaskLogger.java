package com.example.kstream.demo.context.impl;

import com.example.kstream.demo.config.KafkaDefinition;
import com.example.kstream.demo.context.ProcessContext;
import com.example.kstream.demo.context.TaskElement;
import com.example.kstream.demo.model.ClientInsight;
import com.example.kstream.demo.model.SmsNotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

@Order(1)
@Component
public class TaskLogger extends TaskElement<ProcessContext<ClientInsight, SmsNotification>> {

    private final Logger logger  = LoggerFactory.getLogger(TaskLogger.class);
    @Autowired
    private KafkaDefinition kafkaDefinition;
    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    @Override
    public void execute(ProcessContext<ClientInsight, SmsNotification> processContext) {

        logger.debug("This is a simple logger example : {}",processContext );
    }
}
