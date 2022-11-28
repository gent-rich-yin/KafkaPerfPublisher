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
    static int NUM_OF_MESSAGES = 1000;

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
        long stime;

        messages = generateRandomStrings();
        updatePerfMessage("Start publishing...");
        while(true) {
            if( currentTopic == null || !currentTopic.equals(PerfStates.topic)
                    || currentMessageSize == 0 || currentMessageSize != PerfStates.messageSize ) {
                currentTopic = PerfStates.topic;
                currentMessageSize = PerfStates.messageSize;
                if( currentTopic != null && currentMessageSize > 0 ) {
                    updatePerfMessage("Start generating random strings");
                    messages = generateRandomStrings();
                    sleep(1000);
                    updatePerfMessage("Done generating random strings");
                    sleep(1000);
                    updatePerfMessage("Start publishing...");
                    sleep(1000);
                }
            }

            if( currentTopic == null || currentMessageSize <= 0 ) {
                updatePerfMessage("Waiting for valid config assignment");
                sleep(1000);
                continue;
            }

            stime = System.currentTimeMillis();
            int count = 1;
            int messagesSentLastSecond = 0;
            while( !(!currentTopic.equals(PerfStates.topic)
                    || currentMessageSize != PerfStates.messageSize) ) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(PerfStates.topic, Integer.toString(count++), messages[currentMessageIndex++]);
                if( currentMessageIndex >= NUM_OF_MESSAGES ) {
                    currentMessageIndex = 0;
                }
                producer.send(producerRecord);
                messagesSentLastSecond++;
                long ftime = System.currentTimeMillis();
                if( ftime - stime > 1000 ) {
                    updatePerfMessage("messagesSentLastSecond: {0}", messagesSentLastSecond);
                    stime = ftime;
                    messagesSentLastSecond = 0;
                }
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
        String[] strings = new String[NUM_OF_MESSAGES];
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
