package com.example.kstream.demo.context;


import lombok.Data;


@Data
public class ProcessContext <I, O> {

    private Task task;
    private String errorMessage;
    private I input;
    private O output;

}
