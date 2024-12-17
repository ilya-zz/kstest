package model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;

@Data
@NoArgsConstructor
public class Counter {
    private HashMap<String, Integer> counters = new HashMap<>();

    public Counter add(FooEvent e) {
        var c = counters.getOrDefault(e.getName(), 0);
        counters.put(e.getName(), c + 1);
        return this;
    }
}
