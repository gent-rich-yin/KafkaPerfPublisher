package org.example.kafka.perf;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.text.MessageFormat;
import java.util.Properties;

@SpringBootApplication
public class Main {
    static Logger logger = LoggerFactory.getLogger(Main.class);

    KafkaProducer<String, String> producer;
    @Value("${KAFKA.SERVERS}")
    String kafkaServers;
    String[] messages;
    int currentMessageIndex = 0;

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    public void initProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startPublishing() {
        initProducer();

        String currentTopic = null;
        int currentMessageSize = 0;
        int currentMessagePerSecond = 0;
        long stime = System.currentTimeMillis();

        messages = generateRandomStrings();
        updatePerfMessage("Start publishing...");
        while(true) {
            if( currentTopic == null || !currentTopic.equals(PerfStates.topic)
                    || currentMessageSize == 0 || currentMessageSize != PerfStates.messageSize
                    || currentMessagePerSecond == 0 || currentMessagePerSecond != PerfStates.messagesPerSecond ) {
                currentTopic = PerfStates.topic;
                currentMessageSize = PerfStates.messageSize;
                currentMessagePerSecond = PerfStates.messagesPerSecond;
                stime = System.currentTimeMillis();
                if( currentTopic != null && currentMessageSize > 0 && currentMessagePerSecond > 0 ) {
                    updatePerfMessage("Start generating radom strings");
                    messages = generateRandomStrings();
                    sleep(1000);
                    updatePerfMessage("Done generating radom strings");
                    sleep(1000);
                    updatePerfMessage("Start publishing...");
                    sleep(1000);
                }
            }

            if( currentTopic == null || currentMessageSize <= 0 || currentMessagePerSecond <= 0 ) {
                updatePerfMessage("Waiting for valid config assignment");
                sleep(1000);
            }

            stime = System.currentTimeMillis();
            for(int i = 0; i< PerfStates.messagesPerSecond; i++ ) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(PerfStates.topic, Integer.toString(i), messages[currentMessageIndex++]);
                if( currentMessageIndex >= messages.length ) {
                    currentMessageIndex = 0;
                }
                producer.send(producerRecord);
            }
            long ftime = System.currentTimeMillis();

            updatePerfMessage("Published {0} messages in {1}ms.", PerfStates.messagesPerSecond, ftime - stime);
            if( ftime - stime < 1000 ) {
                sleep(1000 - (ftime - stime));
            }
        }
    }

    private static void updatePerfMessage(String s, Object... args) {
        PerfStates.perfMessage = MessageFormat.format(s, args);
        logger.info(PerfStates.perfMessage);
    }

    public static String generateRandomString() {
        char[] letters = "abcdefghijklmnopqrstuvwxyz".toCharArray();
        StringBuilder builder = new StringBuilder();
        for(int i = 0; i< PerfStates.messageSize; i++ ) {
            int index = (int) (Math.random() * 26);
            builder.append(letters[index]);
        }
        return builder.toString();
    }

    public static String[] generateRandomStrings() {
        String[] strings = new String[PerfStates.messagesPerSecond * 10];
        for( int i=0; i<strings.length; i++ ) {
            strings[i] = generateRandomString();
        }
        return strings;
    }

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
