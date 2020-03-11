package com.abhirockzz;


import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import static org.hamcrest.CoreMatchers.*;

import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author abhishekgupta
 */
public class AppTest {

    private TopologyTestDriver td;

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
    public void aggregationShouldProduceValidCount() {

        topology = App.streamAggregation();
        td = new TopologyTestDriver(topology, config);

        TestInputTopic<String, String> inputTopic = td.createInputTopic(App.INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        TestOutputTopic<String, Integer> outputTopic = td.createOutputTopic(App.OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.Integer().deserializer());

        inputTopic.pipeInput("key1", "value1");
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<String, Integer>("key1", 1)));

        inputTopic.pipeInput("key1", "value2");
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<String, Integer>("key1", 2)));

        inputTopic.pipeInput("key2", "value3");
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<String, Integer>("key2", 1)));

        inputTopic.pipeInput("key3", "value4");
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<String, Integer>("key3", 1)));

        inputTopic.pipeInput("key2", "value5");
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<String, Integer>("key2", 2)));

        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void shouldProduceMax() {
        
        topology = App.maxWithReduce();
        td = new TopologyTestDriver(topology, config);
        
        
        TestInputTopic<String, Long> inputTopic = td.createInputTopic(App.INPUT_TOPIC, Serdes.String().serializer(), Serdes.Long().serializer());
        TestOutputTopic<String, Long> outputTopic = td.createOutputTopic(App.OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.Long().deserializer());
        
        String key = "product1";
        
        inputTopic.pipeInput(key, 5L);
        assertThat(outputTopic.readValue(), equalTo(5L));

        inputTopic.pipeInput(key, 6L);
        assertThat(outputTopic.readValue(), equalTo(6L));

        inputTopic.pipeInput(key, 10L);
        assertThat(outputTopic.readValue(), equalTo(10L));

        inputTopic.pipeInput(key, 2L);
        assertThat(outputTopic.readValue(), equalTo(10L));

        inputTopic.pipeInput(key, 5L);
        assertThat(outputTopic.readValue(), equalTo(10L));

        assertThat(outputTopic.isEmpty(), is(true));

    }
}
