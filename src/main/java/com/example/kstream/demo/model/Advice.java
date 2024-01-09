package com.example.kstream.demo.model;

import lombok.Data;
import org.apache.kafka.common.protocol.types.Field;

@Data
public class Advice {

    private Integer IdBala;
    private String zona;
    private String Status;
    private Object[] values;
}
