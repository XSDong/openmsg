package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DefaultPullConsumer implements PullConsumer {
    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;
    private String queue;
    //private Set<String> buckets = new HashSet<>();
    //private List<String> bucketList = new ArrayList<>();
    private Collection<String> topics;
    //private int lastIndex = 0;

    private MessageStoreFile msf = new MessageStoreFile();
    
    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        String data_path = this.properties.getString("STORE_PATH") + "/data_store";
        String conf_path = this.properties.getString("STORE_PATH") + "/conf_store";
        msf.consumerInitial(data_path, conf_path);
    }


    @Override public KeyValue properties() {
        return properties;
    }


    @Override public Message poll() {
       return msf.pullMessage(queue, topics);
    }

    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        this.topics = topics;
    }


}
