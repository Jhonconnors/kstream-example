package com.example.kstream.demo.model;

import lombok.Data;

import java.util.Date;

@Data
public class SmsNotification {

    private String idNotification;
    private String destinationNumber;
    private Date notificationDate;
    private String messageBody;
}
