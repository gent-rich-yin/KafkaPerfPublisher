package org.example.kafka.perf;

import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin(originPatterns = {"localhost", "http://kafka-perf-test.s3-website-us-east-1.amazonaws.com/"})
public class PerfServices {
    @GetMapping("topic")
    public String getTopic() {
        return PerfStates.topic;
    }

    @GetMapping("perfMessage")
    public String getPerfMessage() {
        return PerfStates.perfMessage == null ? "" : PerfStates.perfMessage;
    }

    @PostMapping("topic")
    public void setTopic(@RequestBody(required=false) String topic) {
        PerfStates.topic = topic;
    }

    @GetMapping("messageSize")
    public int getMessageSize() {
        return PerfStates.messageSize;
    }

    @GetMapping("messagesPerSecond")
    public int getMessagesPerSecond() {
        return PerfStates.messagesPerSecond;
    }

    @PostMapping("messageSize")
    public void setMessageSize(@RequestBody String messageSize) {
        PerfStates.messageSize = Integer.parseInt(messageSize);
    }

    @PostMapping("messagesPerSecond")
    public void getMessagesPerSecond(@RequestBody String messagesPerSecond) {
        PerfStates.messagesPerSecond = Integer.parseInt(messagesPerSecond);
    }

}
