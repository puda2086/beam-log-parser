package org.example.entity;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class UserAgent implements Serializable {

    private String name;

    private String version;

    private String info;
}
