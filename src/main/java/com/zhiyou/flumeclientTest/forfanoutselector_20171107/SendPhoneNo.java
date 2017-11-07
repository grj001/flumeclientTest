package com.zhiyou.flumeclientTest.forfanoutselector_20171107;


import java.util.Random;

import com.zhiyou.flumeclientTest.FlumeClient;

public class SendPhoneNo {

	public static void main(String[] args){
		FlumeClient flumeClient = new FlumeClient("master", 8888);
		Random random = new Random();
		for(int i=0;i<100;i++){
			String phoneNo = "1"+random.nextInt(10)+random.nextInt(10)
								+random.nextInt(10)
								+random.nextInt(10)
								+random.nextInt(10)
								+random.nextInt(10)
									+random.nextInt(10)
									+random.nextInt(10)
									+random.nextInt(10)
									+random.nextInt(10);
			flumeClient.sendEvent(phoneNo);
		}
		flumeClient.close();
	}
	
	
	
	
	
	
	
}
