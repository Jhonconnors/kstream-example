package com.example.kstream.demo.context.impl;

import com.example.kstream.demo.context.ProcessContext;
import com.example.kstream.demo.context.TaskElement;
import com.example.kstream.demo.model.ClientInsight;
import com.example.kstream.demo.model.SmsNotification;
import com.example.kstream.demo.model.TaskState;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Random;
import java.util.UUID;


@Order(2)
@Component
public class TaskBuilderSms extends TaskElement<ProcessContext<ClientInsight, SmsNotification>> {
    @Override
    public void execute(ProcessContext<ClientInsight, SmsNotification> processContext) {
        SmsNotification smsNotification = new SmsNotification();
        smsNotification.setIdNotification(UUID.randomUUID().toString());
        generatePhoneNumber(smsNotification);
        smsNotification.setMessageBody("Esta es una Oferta comercial");
        smsNotification.setNotificationDate(new Date());
        processContext.setOutput(smsNotification);
        try {
            // Dormir el hilo durante 15 segundos
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            // Manejo de la excepción en caso de interrupción
            Thread.currentThread().interrupt(); // Restaura el estado de interrupción del hilo
            throw new RuntimeException("El proceso fue interrumpido", e);
        }
        processContext.getTask().setState(TaskState.Success);
    }

    private void generatePhoneNumber(SmsNotification smsNotification) {
        Random random = new Random();
        int number = 100000000 + random.nextInt(900000000);
        smsNotification.setDestinationNumber("+569" + number);
    }
}
