package com.cetcnav.lbs.weixin.service;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;

import com.cetcnav.lbs.weixin.thread.ManageSubThread;


public class NsqSubscriberService {

	private String host;// = "222.222.218.50";
	private int port;// = 4150;
	private String topic;
	private String channel;
	@Autowired
	private ChargePileStatusManager chargePileStatusManager;
	@Autowired
	private ChargeOrderService chargeOrderService;
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	@PostConstruct  
    public void  init(){  
		ManageSubThread mst = new ManageSubThread(host,port,topic,chargePileStatusManager,chargeOrderService,channel);
		mst.start();
	}  
	
}
