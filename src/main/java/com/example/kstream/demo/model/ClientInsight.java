package com.example.kstream.demo.model;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;
import java.util.Date;
import java.util.HashMap;

@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class ClientInsight {

    private Integer id;
    private String offerName;
    private Date timestamp;
    private HashMap<String, String> customerData;
}
