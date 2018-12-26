package com.kafak;



import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProcess {
	public static  Producer<String, String> GetProducer(String kafkaAddress){
		   // create instance for properties to access producer configs   
	    Properties props = new Properties();
	    
	    //Assign localhost id
	    props.put("bootstrap.servers", kafkaAddress);
	    
	    //Set acknowledgements for producer requests.      
	    props.put("acks", "all");
	    
	    //If the request fails, the producer can automatically retry,
	    props.put("retries", 0);
	    
	    //Specify buffer size in config
	    props.put("batch.size", 16384);
	    
	    //Reduce the no of requests less than 0   
	    props.put("linger.ms", 1);
	    
	    //The buffer.memory controls the total amount of memory available to the producer for buffering.   
	    props.put("buffer.memory", 33554432);
	    props.put("partitioner.class", "com.kafka.SimplePartitioner");
	    
	    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
	    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
	    
	    Producer<String, String> producer = new KafkaProducer
	       <String, String>(props);
	    
	     return producer;
	}
	
	public static void Send(Producer<String, String> producer,String id,String topic,String msg) {
		if (producer==null){
			return;
		}
		producer.send(new ProducerRecord<String, String>(topic, 
		          id,msg));
	}

}
