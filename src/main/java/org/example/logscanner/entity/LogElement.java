package org.example.logscanner.entity;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
public class LogElement implements Serializable {

    private String ipAddress;

    private String user;

    private String group;

    private String url;

    private Integer status;

    private List<UserAgent> userAgents;
}
