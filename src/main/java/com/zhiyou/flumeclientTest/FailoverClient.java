package com.zhiyou.flumeclientTest;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

public class FailoverClient {

	private Properties properties;
	private RpcClient failoverClient;

	// initialize RpcClient
	public FailoverClient() throws Exception {
		this.properties = new Properties();
		InputStream inStream = new FileInputStream("D:\\develop\\workspace\\flumeclientTest\\src\\main\\resources\\failover_client.conf");

		properties.load(inStream);

		this.failoverClient = RpcClientFactory.getInstance(properties);
	}

	
	
	// send massage
	public void sendEvent(String msg) {
		// structure event
		Event event = EventBuilder.withBody(msg, Charset.forName("UTF-8"));

		// send massage
		try {
			failoverClient.append(event);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	
	//close client
	public void close() {

		failoverClient.close();
	}

	
	
	public static void main(String[] args) throws Exception {
		FailoverClient failoverClient = new FailoverClient();
		String msg = "message_";
		for(int i=0;i<100;i++){
			failoverClient.sendEvent(msg+i);
			Thread.sleep(1000);;
		}
		failoverClient.close();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
