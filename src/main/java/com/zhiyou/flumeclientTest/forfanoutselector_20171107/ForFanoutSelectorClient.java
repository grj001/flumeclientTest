package com.zhiyou.flumeclientTest.forfanoutselector_20171107;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

public class ForFanoutSelectorClient {

	private RpcClient client;
	private final String[] provinces = 
		{"henan","hebei","shandong","shanghai","beijing"};
	private final Random random = 
			new Random();
	
	//random create event`s province
	public ForFanoutSelectorClient(String hostname, int port){
		this.client = RpcClientFactory.getDefaultInstance(hostname, port);
	}
	
	
	public Event getRandomEvent(String msg){
		Map<String ,String> headers = new HashMap<String , String>();
		
		String province = provinces[random.nextInt(5)];
		
		headers.put("province", province);
		Event result = 
				EventBuilder.withBody(msg+province,Charset.forName("UTF-8"), headers);
		return result;
	}
	
	public void sendEvent(Event event){
		try {
			client.append(event);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public void close(){
		client.close();
	}
	
	
	public static void main(String[] args) {
		ForFanoutSelectorClient fanoutSelectorClient = 
				new ForFanoutSelectorClient("master",8888);
		String msg = "peopleinfo_";
		for(int i=0;i<300;i++){
			Event event = fanoutSelectorClient.getRandomEvent(msg+i+"_");
			System.out.println(event);
			fanoutSelectorClient.sendEvent(event);
		}
		fanoutSelectorClient.close();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
