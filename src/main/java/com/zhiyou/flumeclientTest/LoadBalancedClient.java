package com.zhiyou.flumeclientTest;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

public class LoadBalancedClient {

	private RpcClient lbClient;
	private Properties properties;
	
	public LoadBalancedClient() throws FileNotFoundException, IOException {
		this.properties = new Properties();
		properties.load(new FileInputStream("src/main/resources/load_balance.conf"));
		
		this.lbClient = RpcClientFactory.getInstance(properties);
	}
	
	public void sendEvent(String msg){
		Event event = EventBuilder.withBody(msg,Charset.forName("UTF-8"));
		// send massage
		try {
			lbClient.append(event);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	//close client
	public void close() {

		lbClient.close();
	}

	
	
	public static void main(String[] args) throws Exception {
		LoadBalancedClient lbClient = new LoadBalancedClient();
		String msg = "lbmessage_";
		for(int i=0;i<100;i++){
			lbClient.sendEvent(msg+i);
			Thread.sleep(1000);;
		}
		lbClient.close();
	}
	
}
