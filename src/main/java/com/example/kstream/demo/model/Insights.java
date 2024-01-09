package com.example.kstream.demo.model;

import lombok.Data;

import java.util.Date;

@Data
public class Insights {

    private Integer id;
    private String variante;
    private Date timeStamp;
    private Object[] variables;
}
