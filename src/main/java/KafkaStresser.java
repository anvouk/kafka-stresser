import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaStresser {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStresser.class);

    private final String kafkaServers;

    private final AtomicBoolean isRunning = new AtomicBoolean();

    public KafkaStresser(String kafkaServers) {
        this.kafkaServers = kafkaServers;
    }

    private Producer<String, String> newProducer(int clientId) {
        Properties props = new Properties();
        props.put("client.id", String.format("prod-%d", clientId));
        props.put("acks", "1");
        props.put("max.in.flight.requests.per.connection", 1);
        props.put("bootstrap.servers", kafkaServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private Consumer<String, String> newConsumer(int clientId) {
        Properties props = new Properties();
        props.put("client.id", String.format("consumer-%d", clientId));
        props.put("bootstrap.servers", kafkaServers);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "kcoordinator");
        props.put("max.poll.records", "50");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    private void Produce(String topic, int partition) {
        Thread t = new Thread(() -> {
            Producer<String, String> kafkaProducer = newProducer(partition);
            Random rand = new Random();
            while (isRunning.get()) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) { }
                String value = Integer.toString(rand.nextInt());
                try {
                    kafkaProducer.send(new ProducerRecord<>(topic, partition, null, null, value)).get();
                } catch (Exception ignored) { }
                logger.info("[{}:{}] sent: {}", topic, partition, value);
            }
            logger.info("stopping producer {}:{}", topic, partition);
            kafkaProducer.close(Duration.ofSeconds(3));
        });
        t.setName(String.format("Producer Thread %s:%d", topic, partition));
        t.start();
    }

    private void Consume(String topic, int partition) {
        Thread t = new Thread(() -> {
            Consumer<String, String> kafkaConsumer = newConsumer(partition);
            kafkaConsumer.assign(Collections.singletonList(new TopicPartition(topic, partition)));
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("[{}:{}] consumed({}): {}", record.topic(), record.partition(), record.offset(), record.value());
                    HashMap<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
                    commitMap.put(new TopicPartition(topic, partition), new OffsetAndMetadata(record.offset() + 1));
                    kafkaConsumer.commitSync(commitMap);
                }
            }
            logger.info("stopping consumer {}:{}", topic, partition);
            kafkaConsumer.close(Duration.ofSeconds(3));
        });
        t.setName(String.format("Consumer Thread %s:%d", topic, partition));
        t.start();
    }

    private void Work(String topic, int partitions) {
        for (int i = 0; i < partitions; ++i) {
            Consume(topic, i);
            Produce(topic, i);
        }
    }

    public void start(int topicsCount, int partitionsPerTopic) {
        isRunning.set(true);
        for (int i = 0; i < topicsCount; ++i) {
            Work(String.format("TOPIC_%d", i), partitionsPerTopic);
        }
    }

    public void stop() {
        isRunning.set(false);
    }
}
