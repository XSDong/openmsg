package io.openmessaging.demo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import io.openmessaging.*;

public class MessageStoreFile {
	private static RandomAccessFile raf = null;//producer and consumer 
	public static RandomAccessFile raf_conf = null;//producer and consumer 
	public static final long fileLength = 6000l; //producer
	public static long BlocksLength = 0; //producer and consumer
	public static int producerBlockSize =  200;//mappebytebuffer 只能映射int表示的范围。
    public static int consumerBlockSize = producerBlockSize*2;
    public static String data_path = null;
    public static String conf_path = null;
    
    private RandomAccessFile threadRaf = null; //producer and consumer
    private FileChannel threadfc = null; //producer and consumer
    private long curBlocksLength = 0; //consumer
    private MappedByteBuffer mbb; //producer and consumer
    
    public void producerInitial(String data_path,String conf_path){
    	synchronized(MessageStoreFile.class){
    		if(raf == null){
    			try {
    				MessageStoreFile.data_path = data_path;
    				MessageStoreFile.conf_path = conf_path;
					raf = new RandomAccessFile(MessageStoreFile.data_path,"rw");
					raf.setLength(MessageStoreFile.fileLength);
					raf_conf = new RandomAccessFile(MessageStoreFile.conf_path,"rw");
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
    		}
    	}
		try {
			threadRaf = new RandomAccessFile(data_path,"rw");
			threadfc = threadRaf.getChannel();
			mbb = mapNewRegionWhenPut();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
    }
    public void producerPutConfFlush(){
    	synchronized(MessageStoreFile.class){
    		//if(raf_conf != null){
    			try {
					raf_conf.writeLong(BlocksLength);
				} catch (IOException e) {
					e.printStackTrace();
				}
    			//raf_conf = null;
    		//}
    	}
    }
    public void putEndFlush(){
    	if(mbb.remaining() >= 4)mbb.putInt(-1);
    }
    public MappedByteBuffer putMessage(String bucket, Message message){
    	BytesMessage msg = (BytesMessage)message;
    	ByteBuffer bb = Operation.encodeKeyValuesToBytes(msg);
    	int length = bb.limit() + msg.getBody().length;
    	//System.out.println("length is:"+length);
    	if(mbb.remaining() < 4){
    		mbb = mapNewRegionWhenPut();
    	}
    	else if(mbb.remaining() < length + 4){
    		mbb.putInt(-1);
    		mbb = mapNewRegionWhenPut();
    	}
    	mbb.putInt(length);
    	mbb.put(bb);
    	mbb.put(msg.getBody());
    	return mbb;
    }
    public MappedByteBuffer mapNewRegionWhenPut(){
    	long begin;
    	synchronized(MessageStoreFile.class){
    		begin = BlocksLength;
    		BlocksLength += producerBlockSize;
    	}
    	try {
			mbb = threadfc.map(MapMode.READ_WRITE, begin,producerBlockSize);
		} catch (IOException e) {
			e.printStackTrace();
		} 
    	return mbb;
    }
    
    public RandomAccessFile consumerInitial(String data_path,String conf_path){
    	synchronized(this){
    		if(raf_conf == null){
    			try {
					raf_conf = new RandomAccessFile(conf_path,"rw");
					BlocksLength = raf_conf.readLong();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
    		}
    	}
		try {
			threadRaf = new RandomAccessFile(data_path,"rw");
			threadfc = threadRaf.getChannel();
			curBlocksLength = 0;
			mbb = mapNewRegionWhenPull();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
    	return threadRaf;
    }
    
    public Message pullMessage(String queueName, Collection<String> topics){
    	BytesMessage msg = null;
    	int offsetKeyValues = 0;
    	int length = 0;
    	while(msg == null){
	    	if(mbb.remaining() < 4){
	    		mbb = mapNewRegionWhenPull();
	    		if(mbb == null) return null;
	    	}
	    	else{
	    		length = mbb.getInt();
	    		if(length == -1){
	    			int remainingProducerBlocksNum = (mbb.remaining() - mbb.position())/producerBlockSize ;
	    			if(remainingProducerBlocksNum == 0){
	    				mbb = mapNewRegionWhenPull();
	    				if(mbb == null) return null;
	    			}
	    			else{
	    				int newPosition = producerBlockSize*((mbb.position()+producerBlockSize)/producerBlockSize);
	    				mbb.position(newPosition);
	    			}
	    			length = mbb.getInt();
	    		}
	    		int curPosition = mbb.position();
	    		msg = Operation.decodeKeyValuesFromBytes(mbb,queueName,topics);
	    		if(msg == null){
	    			mbb.position(curPosition+length);
	    		}
	    		else {
	    			offsetKeyValues = mbb.position() - curPosition;
	    			break;
	    		}
	    	}
    	}
    	byte[] body = new byte[length-offsetKeyValues];
    	mbb.get(body);
    	msg.setBody(body);
    	return msg;
    }
    public MappedByteBuffer mapNewRegionWhenPull(){
    	MappedByteBuffer mbb = null;
    	if(BlocksLength == curBlocksLength)return null;
    	try {
    		 long length = (consumerBlockSize <= (BlocksLength - curBlocksLength))?
    				consumerBlockSize:(BlocksLength - curBlocksLength);
			mbb = threadfc.map(MapMode.READ_WRITE, curBlocksLength,(int)length);
			curBlocksLength += length;
		} catch (IOException e) {
			e.printStackTrace();
		} 
    	return mbb; 	
    }
}

class Operation{
	public static String coding = "GBK";
	public static int commonStringBufferLimit = 1000;
	public static int keyStringBufferLimit = 200;
	public static int keyValuesArrayByteBufferLimit = 1000;
	public static byte topicByte = 1;
	public static byte queueByte = 2;
	public static int encodeStringToBytes(ByteBuffer bb,String str){//用short，没用int
		byte[] bytesArray = str.getBytes();
		int length = bytesArray.length;
		bb.putShort((short) length);
		bb.put(bytesArray);
		return length + 4;
	}
	public static String decodeStringFromBytes(ByteBuffer bb){//用short，没用int
		byte[] temp = new byte[Operation.commonStringBufferLimit];
		short length = bb.getShort();
		System.out.println("in string decoder: " + length + " " + (bb.limit()-bb.position() + 1));
		bb.get(temp, 0, length);
		String result = null;
		try {
			result =  new String(temp,0,length,Operation.coding);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return result;
	}
	public static int encodeKeyToBytes(ByteBuffer bb,String str){
		byte[] bytesArray = str.getBytes();
		int length = bytesArray.length;
		bb.put((byte)length);
		bb.put(bytesArray);
		return length + 1;
	}
	public static String decodeKeyFromBytes(ByteBuffer bb){
		byte[] temp = new byte[Operation.keyStringBufferLimit];
		byte length = bb.get();
		bb.get(temp, 0, length);
		String result = null;
		try {
			result =  new String(temp,0,length,Operation.coding);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public static void encodeOneKeyValueToBytes(ByteBuffer bb,DefaultKeyValue kv){
		if(kv==null){
			bb.put((byte)-1);
			return;
		}
		Set<Map.Entry<String, Integer>> set_int = kv.entrySetInteger();
		bb.put((byte)set_int.size());
		for (Map.Entry<String, Integer> entry : set_int) {  	  
		      encodeKeyToBytes(bb,entry.getKey());
		      bb.putInt(entry.getValue());
		} 
	    Set<Map.Entry<String, Long>> set_long = kv.entrySetLong();
		bb.put((byte)set_long.size());
		for (Map.Entry<String,Long> entry : set_long) {  	  
		      encodeKeyToBytes(bb,entry.getKey());
		      bb.putLong(entry.getValue());
		} 
		Set<Map.Entry<String, Double>> set_double = kv.entrySetDouble();
		bb.put((byte)set_double.size());
		for (Map.Entry<String, Double> entry : set_double) {  	  
		      encodeKeyToBytes(bb,entry.getKey());
		      bb.putDouble(entry.getValue());
		} 
		Set<Map.Entry<String, String>> set_string = kv.entrySetString();
		bb.put((byte)set_string.size());
		for (Map.Entry<String, String> entry : set_string) {  	  
		      encodeKeyToBytes(bb,entry.getKey());
		      encodeStringToBytes(bb,entry.getValue());
		} 
	}
	
	public static ByteBuffer encodeKeyValuesToBytes(BytesMessage msg
			){//将msg中的KeyValues 编码,（并且topic字段重复了两次）
		ByteBuffer bb = ByteBuffer.allocate(keyValuesArrayByteBufferLimit);
		DefaultKeyValue headers = (DefaultKeyValue) msg.headers();
		String topicOrQueue = headers.getString(MessageHeader.TOPIC);
		if(topicOrQueue!=null){
			bb.put(topicByte);
			encodeStringToBytes(bb,topicOrQueue);
			//headers.removeStringValue(MessageHeader.TOPIC);
		}
		else {
			topicOrQueue = headers.getString(MessageHeader.QUEUE);
			bb.put(queueByte);
			encodeStringToBytes(bb,topicOrQueue);
			//headers.removeStringValue(MessageHeader.QUEUE);
		}
		encodeOneKeyValueToBytes(bb,headers);
		encodeOneKeyValueToBytes(bb,(DefaultKeyValue)msg.properties());
		bb.flip();
		return bb;
	}
	
	public static void decodeOneKeyValueFromBytes(ByteBuffer bb,DefaultKeyValue kv){
		byte num_int = bb.get();
		if(num_int == -1)return;
		for(int i=0;i<num_int;i++){
			String key = decodeKeyFromBytes(bb);
			int value = bb.getInt();
			kv.put(key, value);
		}
		byte num_long = bb.get();
		for(int i=0;i<num_long;i++){
			String key = decodeKeyFromBytes(bb);
			long value = bb.getLong();
			kv.put(key, value);
		}
		byte num_double = bb.get();
		for(int i=0;i<num_double;i++){
			String key = decodeKeyFromBytes(bb);
			double value = bb.getDouble();
			kv.put(key, value);
		}
		byte num_string = bb.get();
		for(int i=0;i<num_string;i++){
			String key = decodeKeyFromBytes(bb);
			String value = decodeStringFromBytes(bb);
			kv.put(key, value);
		}
	}
	
	public static BytesMessage decodeKeyValuesFromBytes(ByteBuffer bb,
			String queueName, Collection<String> topics){//解码KeyValues到msg的相关字段，如果不在topic或queue中则返回null
		byte type = bb.get();
		String topicOrQueue = decodeStringFromBytes(bb);
		if(type == queueByte && !topicOrQueue.equals(queueName))return null;
		if(type == topicByte && !topics.contains(topicOrQueue)) return null;
		DefaultBytesMessage msg = new DefaultBytesMessage(null);//body暂且不管，现在关系keyvalue
				
		if(type == queueByte) msg.putHeaders(MessageHeader.QUEUE, topicOrQueue);
		else msg.putHeaders(MessageHeader.TOPIC, topicOrQueue);
		decodeOneKeyValueFromBytes(bb,(DefaultKeyValue) msg.headers());
		decodeOneKeyValueFromBytes(bb,(DefaultKeyValue) msg.properties());
		return msg;
	}
	
	
}
