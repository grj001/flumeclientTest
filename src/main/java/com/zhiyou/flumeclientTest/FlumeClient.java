package com.zhiyou.flumeclientTest;

import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

//avro   e
//connection avro, send event to flume agent
public class FlumeClient {

	
	private RpcClient flumeClient;
	private String hostName;
	private int port;

	public FlumeClient(
			String hostName 
			, int port){
		this.hostName = hostName;
		this.port = port;
		this.flumeClient = 
				RpcClientFactory
				.getDefaultInstance(hostName, port);
	}
	
	
	//发送event 到avro source
	public void sendEvent(String msg){
		//structure header
		Map<String, String> headers = 
				new HashMap<>();
		headers.put("timestamp", String.valueOf(new Date().getTime()));
		//structure event
		Event event = 
				EventBuilder.withBody(msg, Charset.forName("UTF-8"), headers);
		
		//send massage
		try {
			flumeClient.append(event);
		} catch (Exception e) {
			e.printStackTrace();
			// while one massage fail to send , then reconnection
			flumeClient.close();
			flumeClient = null;
			flumeClient = RpcClientFactory.getDefaultInstance(hostName, port);
		}
		
		
	}
	
	//close the flumeClient connection
	public void close(){
		
		flumeClient.close();
	}
	
	
	
	public static void main(String[] args) {
		
		FlumeClient flumeClient = new FlumeClient("master", 8888);
		String bMsg = "fromjava-msg";
		for(int i=0;i<100;i++){
			flumeClient.sendEvent(bMsg+i);
		}
		flumeClient.close();
	}
	
}
