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
        PerfStates.topic = args[0];
        PerfStates.messageSize = Integer.parseInt(args[1]);
        PerfStates.messagesPerSecond = Integer.parseInt((args[2]));
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
        messages = generateRandomStrings();
        updatePerfMessage("Start publishing...");
        while(true) {
            long stime = System.currentTimeMillis();
            for(int i = 0; i< PerfStates.messagesPerSecond; i++ ) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(PerfStates.topic, Integer.toString(i), messages[currentMessageIndex++]);
                if( currentMessageIndex >= messages.length ) {
                    currentMessageIndex = 0;
                }
                producer.send(producerRecord);
            }
            long ftime = System.currentTimeMillis();

            updatePerfMessage("Published {} messages in {}ms.", PerfStates.messagesPerSecond, ftime - stime);
            if( ftime - stime < 1000 ) {
                try {
                    Thread.sleep(1000 - (ftime - stime));
                } catch (InterruptedException e) {
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
        String[] strings = new String[PerfStates.messagesPerSecond * 10];
        for( int i=0; i<strings.length; i++ ) {
            strings[i] = generateRandomString();
        }
        return strings;
    }
}
