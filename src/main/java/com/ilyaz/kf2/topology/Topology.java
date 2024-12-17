package com.ilyaz.kf2.topology;

import lombok.extern.slf4j.Slf4j;
import model.Counter;
import model.FooEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.time.Instant;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

@EnableKafka
@EnableKafkaStreams
@Slf4j
@Configuration
public class Topology {

    @Autowired
    void topology(StreamsBuilder sb) {

        var s = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(FooEvent.class));
        var sCounter = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Counter.class));

        var win = TimeWindows.ofSizeAndGrace(Duration.ofSeconds(30), Duration.ofSeconds(2));

//        StoreBuilder<KeyValueStore<Windowed<String>, Counter>> countStoreSupplier =
//                Stores.keyValueStoreBuilder(
//                        Stores.persistentKeyValueStore("ag-win"),
//                        Serdes.String(),
//                        sCounter);
//        KeyValueStore<String, Long> countStore = countStoreSupplier.build();

        sb.stream("kf2-in", Consumed.with(Serdes.String(), s).withTimestampExtractor(new FooEvent.TSExtractor()))
                .mapValues(x -> new FooEvent(x.getName().toUpperCase(),x.getId() * 10, x.getEventTime(), x.getSeconds()))
                .groupByKey()
                .windowedBy(win)
//                .emitStrategy(EmitStrategy.onWindowClose())
                .aggregate(Counter::new, (k, e, c) ->  c.add(e),
                        Named.as("ag-win"),
                        Materialized.with(Serdes.String(), sCounter)
                        )
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .process(FooProcessor::new, Named.as("myproc"), "KSTREAM-AGGREGATE-STATE-STORE-0000000002")
                .selectKey((k, v) -> k.key())
                .to("kf2-out", Produced.with(Serdes.String(), sCounter));
    }

    @Slf4j
    static class FooProcessor implements Processor<Windowed<String>, Counter, Windowed<String>, Counter> {
        private ProcessorContext<Windowed<String>, Counter> context;

        @Override
        public void init(ProcessorContext<Windowed<String>, Counter> context) {
            this.context = context;

            TimestampedWindowStore<String, Counter> store = context.getStateStore("KSTREAM-AGGREGATE-STATE-STORE-0000000002");

            context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, ts -> {
                try (final var iter = store.all()) {
                    var t0 = Instant.now();
                    var n = 0;
                    while (iter.hasNext()) {
                        n ++;
                        final KeyValue<Windowed<String>, ValueAndTimestamp<Counter>> entry = iter.next();
//                        log.info("my proc: {} -> {} (cur: {})", entry.key, entry.value, context.currentStreamTimeMs());
                        if (entry.key.window().end() + 20000 < context.currentStreamTimeMs()) {
                            log.info("flush {} with ts {}", entry.key, entry.key.window().start());
                            store.put(entry.key.key(), null, entry.key.window().start());
                            var rec = new Record<Windowed<String>, Counter>(entry.key, entry.value.value(), entry.value.timestamp());
                            context.forward(rec);
                        }
                    }
                    log.info("my proc: done {} items in {}", n, Duration.between(t0, Instant.now()).toMillis());
                } catch (Exception e) {
                    log.error("boom", e);
                }
            });
        }
        @Override
        public void process(Record<Windowed<String>, Counter> record) {
            context.forward(record);
        }
        @Override
        public void close() {
        }
    }
}
