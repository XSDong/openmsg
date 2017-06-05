package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DefaultKeyValue implements KeyValue {

    private final Map<String, Integer> kvs_int = new HashMap<>();
    private final Map<String, String> kvs_string = new HashMap<>();
    private final Map<String, Long> kvs_long = new HashMap<>();
    private final Map<String, Double> kvs_double = new HashMap<>();
    @Override
    public KeyValue put(String key, int value) {
        kvs_int.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, long value) {
        kvs_long.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, double value) {
        kvs_double.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, String value) {
        kvs_string.put(key, value);
        return this;
    }

    @Override
    public int getInt(String key) {
        return kvs_int.getOrDefault(key, 0);
    }

    @Override
    public long getLong(String key) {
        return kvs_long.getOrDefault(key, 0L);
    }

    @Override
    public double getDouble(String key) {
        return kvs_double.getOrDefault(key, 0.0d);
    }

    @Override
    public String getString(String key) {
        return (String)kvs_string.getOrDefault(key, null);
    }

    @Override
    public Set<String> keySet() {
    	Set<String> result = new HashSet<String>();
        result.addAll(kvs_int.keySet());
        result.addAll(kvs_long.keySet());
        result.addAll(kvs_double.keySet());
        result.addAll(kvs_string.keySet());
        return result;
    }

    @Override
    public boolean containsKey(String key) {
    	if(kvs_int.containsKey(key))return true;
    	if(kvs_long.containsKey(key))return true;
    	if(kvs_double.containsKey(key))return true;
    	if(kvs_string.containsKey(key))return true;
        return false;
    }
    
    public String removeStringValue(String key){
    	return kvs_string.remove(key);
    }
    
    public Set<Map.Entry<String, Integer>> entrySetInteger(){
    	return kvs_int.entrySet();
    }
    public Set<Map.Entry<String, Long>> entrySetLong(){
    	return kvs_long.entrySet();
    }
    public Set<Map.Entry<String, Double>> entrySetDouble(){
    	return kvs_double.entrySet();
    }
    public Set<Map.Entry<String, String>> entrySetString(){
    	return kvs_string.entrySet();
    }
    
    public Map<String,Integer> mapInt(){
    	return kvs_int;
    }
    public Map<String,Long> mapLong(){
    	return kvs_long;
    }
    public Map<String,Double> mapDouble(){
    	return kvs_double;
    }
    public Map<String,String> mapString(){
    	return kvs_string;
    }
    
    @Override
    public boolean equals(Object obj){
    	if(obj==null)return false;
    	DefaultKeyValue kv = (DefaultKeyValue)obj;
    	return kv.mapInt().equals(kvs_int) && kv.mapLong().equals(kvs_long)
    			&& kv.mapDouble().equals(kvs_double) && kv.mapString().equals(kvs_string);
    }
    
    public Set<String> keySetInt(){
    	return kvs_int.keySet();
    }
    public Set<String> keySetLong(){
    	return kvs_long.keySet();
    }
    public Set<String> keySetDouble(){
    	return kvs_double.keySet();
    }
    public Set<String> keySetString(){
    	return kvs_string.keySet();
    }
    
    public void debugOutput(){
    	for(Map.Entry<String, Integer> entry:kvs_int.entrySet()){
    		System.out.print(entry.getKey() + ":" + entry.getValue()+" ");
    	}
    	System.out.println();
    	for(Map.Entry<String,Long> entry:kvs_long.entrySet()){
    		System.out.print(entry.getKey() + ":" + entry.getValue()+" ");
    	}
    	System.out.println();
    	for(Map.Entry<String, Double> entry:kvs_double.entrySet()){
    		System.out.print(entry.getKey() + ":" + entry.getValue()+" ");
    	}
    	System.out.println();
    	for(Map.Entry<String, String> entry:kvs_string.entrySet()){
    		System.out.print(entry.getKey() + ":" + entry.getValue()+" ");
    	}
    	System.out.println();
    	System.out.println();
    }
}
