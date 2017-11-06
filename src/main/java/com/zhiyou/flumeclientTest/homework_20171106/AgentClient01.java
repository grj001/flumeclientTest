package com.zhiyou.flumeclientTest.homework_20171106;

import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;


public class AgentClient01 {

	private RpcClient agentClient01;
	private String hostName;
	private int port;

	
	// initailized Client
	public AgentClient01(
			String hostName 
			, int port){
		this.hostName = hostName;
		this.port = port;
		this.agentClient01 = 
				RpcClientFactory
				.getDefaultInstance(hostName, port);
	}
	
	
	//发送event 到avro source
	public void sendEvent(String msg){
		//structure header
		Map<String, String> headers = 
				new HashMap<String, String>();
		headers.put("timestamp", String.valueOf(new Date().getTime()));
		//structure event
		Event event = 
				EventBuilder.withBody(msg, Charset.forName("UTF-8"), headers);
		
		//send massage
		try {
			agentClient01.append(event);
		} catch (Exception e) {
			e.printStackTrace();
			// while one massage fail to send , then reconnection
			agentClient01.close();
			agentClient01 = null;
			agentClient01 = RpcClientFactory.getDefaultInstance(hostName, port);
		}
		
		
	}
	
	//close the flumeClient connection
	public void close(){
		
		agentClient01.close();
	}
	
	
	
	public static void main(String[] args) throws InterruptedException {
		
		AgentClient01 agentClient01 = new AgentClient01("master", 8888);
		String bMsg = "fromjava-msg";
		for(int i=0;i<100;i++){
			agentClient01.sendEvent(bMsg+i);
			Thread.sleep(0);
		}
		agentClient01.close();
	}
}
