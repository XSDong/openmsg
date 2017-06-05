package io.openmessaging.demo;

import java.util.Arrays;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

public class DefaultBytesMessage implements BytesMessage {

    private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties = new DefaultKeyValue();
    private byte[] body;

    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }
    @Override public byte[] getBody() {
        return body;
    }

    @Override public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override public KeyValue headers() {
        return headers;
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }
    
    @Override
    public boolean equals(Object obj){
    	if(obj==null)return false;
    	DefaultBytesMessage msg = (DefaultBytesMessage)obj;
    	if(!headers.equals(msg.headers()))return false;
    	if(properties == null){
    		if(msg.properties() != null)return false;
    	}
    	else{
    		if(!properties.equals(msg.properties())) return false;
    	}
    	if(!Arrays.equals(body, msg.getBody()))return false;
    	return true;
    }
    
    public void debugOutput(){
    	System.out.println("message begin");
    	((DefaultKeyValue)headers).debugOutput();
    	if(properties != null)((DefaultKeyValue)properties).debugOutput();
    	for(int i=0;i<body.length;i++){
    		System.out.print(body[i] + " ");
    	}
    	System.out.println("message end");
    }
}
