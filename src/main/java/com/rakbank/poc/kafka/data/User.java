package com.rakbank.poc.kafka.data;

import lombok.Data;

@Data
public class User {
    private int id;
    private String name;
    private String[] address;

}