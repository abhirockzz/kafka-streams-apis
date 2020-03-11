
package com.abhirockzz;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 *
 * @author abhishekgupta
 */
public class KeyCountSerde implements Serde<Count>  {

    @Override
    public Serializer<Count> serializer() {
        return new Serializer<Count>() {
            @Override
            public byte[] serialize(String string, Count kc) {
                String s = kc.getKey() + "," + kc.getCount();
                return s.getBytes();
            }
        };
    }

    @Override
    public Deserializer<Count> deserializer() {
        return new Deserializer<Count>() {
            @Override
            public Count deserialize(String string, byte[] bytes) {
                String s = new String(bytes);
                return new Count(s.split(",")[0], Integer.parseInt(s.split(",")[1]));
            }
        };
    }

}
