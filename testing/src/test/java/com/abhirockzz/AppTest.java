package com.abhirockzz;

import com.abhirockzz.App;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import static org.hamcrest.CoreMatchers.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author abhishekgupta
 */
public class AppTest {

    private TopologyTestDriver td;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    private Topology topology;
    private final Properties config;

    public AppTest() {

        config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, App.APP_ID);
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "foo:1234");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    }

    @After
    public void tearDown() {
        td.close();
    }

    @Test
    public void shouldIncludeValueWithLengthGreaterThanFive() {

        topology = App.retainWordsLongerThan5Letters();
        td = new TopologyTestDriver(topology, config);

        inputTopic = td.createInputTopic(App.INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = td.createOutputTopic(App.OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.String().deserializer());

        assertThat(outputTopic.isEmpty(), is(true));

        inputTopic.pipeInput("foo", "barrrrr");
        assertThat(outputTopic.readValue(), equalTo("barrrrr"));
        assertThat(outputTopic.isEmpty(), is(true));

        inputTopic.pipeInput("foo", "bar");
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void testFlatMap() {
        topology = App.flatMap();
        td = new TopologyTestDriver(topology, config);

        inputTopic = td.createInputTopic(App.INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = td.createOutputTopic(App.OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.String().deserializer());

        inputTopic.pipeInput("random", "foo,bar,baz");
        inputTopic.pipeInput("hello", "world,universe");
        inputTopic.pipeInput("hi", "there");

        assertThat(outputTopic.getQueueSize(), equalTo(6L));

        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("random", "foo")));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("random", "bar")));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("random", "baz")));

        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("hello", "world")));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("hello", "universe")));

        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("hi", "there")));

        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void shouldGroupRecordsAndProduceValidCount() {
        topology = App.count();
        td = new TopologyTestDriver(topology, config);

        inputTopic = td.createInputTopic(App.INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        TestOutputTopic<String, Long> ot = td.createOutputTopic(App.OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.Long().deserializer());

        inputTopic.pipeInput("key1", "value1");
        inputTopic.pipeInput("key1", "value2");
        inputTopic.pipeInput("key2", "value3");
        inputTopic.pipeInput("key3", "value4");
        inputTopic.pipeInput("key2", "value5");

        assertThat(ot.readKeyValue(), equalTo(new KeyValue<String, Long>("key1", 1L)));
        assertThat(ot.readKeyValue(), equalTo(new KeyValue<String, Long>("key1", 2L)));
        assertThat(ot.readKeyValue(), equalTo(new KeyValue<String, Long>("key2", 1L)));
        assertThat(ot.readKeyValue(), equalTo(new KeyValue<String, Long>("key3", 1L)));
        assertThat(ot.readKeyValue(), equalTo(new KeyValue<String, Long>("key2", 2L)));

        assertThat(ot.isEmpty(), is(true));
    }

    @Test
    public void shouldProduceValidCountInStateStore() {
        topology = App.countWithStateStore();
        td = new TopologyTestDriver(topology, config);

        inputTopic = td.createInputTopic(App.INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        
        KeyValueStore<String, Long> countStore = td.getKeyValueStore("count-store");

        inputTopic.pipeInput("key1", "value1");
        assertThat(countStore.get("key1"), equalTo(1L));

        inputTopic.pipeInput("key1", "value2");
        assertThat(countStore.get("key1"), equalTo(2L));

        inputTopic.pipeInput("key2", "value3");
        assertThat(countStore.get("key2"), equalTo(1L));

        inputTopic.pipeInput("key3", "value4");
        assertThat(countStore.get("key3"), equalTo(1L));

        inputTopic.pipeInput("key2", "value5");
        assertThat(countStore.get("key2"), equalTo(2L));
    }
}
