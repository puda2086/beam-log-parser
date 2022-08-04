package org.example.transform;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.example.entity.LogElement;
import org.example.entity.UserAgent;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class ParseLog extends DoFn<String, LogElement> {

    private final Pattern logPattern = Pattern.compile("(?<ip>\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}) (?<user>-|\\w*) (?<group>-|\\w*) \\[(?<timestamp>\\d{2}/\\w{3}\\/\\d{4}:\\d{2}:\\d{2}:\\d{2} \\+\\d{4})\\] \"(?<method>[A-Z]*) (?<host>.*) (?<proto>.*)\" (?<status>\\d{3}) \\d{4} \"-\" \"(?<useragents>.*)\"(?<junkdata>.*)");

    private final Pattern userAgentPattern = Pattern.compile("(?<name>.*?)\\/(?<version>.*?)(?:(\\s|$)\\((?<info>.*?)\\))?(\\s|$)");

    @ProcessElement
    public void processElement(@Element String logString, OutputReceiver<LogElement> receiver) {

        Matcher logMatcher = logPattern.matcher(logString);

        if (logMatcher.matches()) {

            Matcher agentMatcher = userAgentPattern.matcher(logMatcher.group("useragents"));

            List<UserAgent> userAgents = new ArrayList<>();

            while (agentMatcher.find()) {
                userAgents.add(UserAgent.builder()
                        .name(agentMatcher.group("name"))
                        .version(agentMatcher.group("version"))
                        .info(agentMatcher.group("info"))
                        .build());
            }

            LogElement logElement = LogElement.builder()
                    .ipAddress(logMatcher.group("ip"))
                    .url(logMatcher.group("host"))
                    .user(logMatcher.group("user"))
                    .group(logMatcher.group("group"))
                    .status(Integer.parseInt(logMatcher.group("status")))
                    .userAgents(userAgents)
                    .build();

            receiver.output(logElement);
        } else {
            log.error("Incorrectly formatted log entry: {}", logString);
        }

    }
}
