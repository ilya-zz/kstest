package model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerializer;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class FooEvent {
    private String name;
    private int id;
    private Instant eventTime;
    private int seconds;

    static public class TSExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            var f = (FooEvent)record.value();
            log.info("extract ts: {} sec, stream time: {}", f.seconds, partitionTime);
            return f.seconds * 1000L;
        }
    }
}
