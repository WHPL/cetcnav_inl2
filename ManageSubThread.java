package com.cetcnav.lbs.weixin.thread;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.nsqjava.core.NSQFrameDecoder;

import com.cetcnav.lbs.weixin.handler.ManageHandler;
import com.cetcnav.lbs.weixin.service.ChargeOrderService;
import com.cetcnav.lbs.weixin.service.ChargePileStatusManager;

public class ManageSubThread extends Thread  {

	private String host;// = "222.222.218.50";
	private int port ;//= 4150;
	private ChargePileStatusManager chargePileStatusManager;
	private ChargeOrderService chargeOrderService;
	private ChannelFactory factory;
	private String topic;
	private String channel;
	private InetSocketAddress addr;
	private ClientBootstrap bootstrap;
	public ManageSubThread(String host, int port, String topic, ChargePileStatusManager chargePileStatusManager, ChargeOrderService chargeOrderService,String channel){
		this.host = host;
		this.port = port;
		this.chargePileStatusManager = chargePileStatusManager;
		this.chargeOrderService = chargeOrderService;
		this.topic = topic;
		this.channel = channel;
	}
	
	public void run() {  

		
		// connect to nsqd TODO add step for lookup via nsqlookupd
		factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());

		bootstrap = new ClientBootstrap(factory);
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() {
				return Channels.pipeline(new NSQFrameDecoder(),new ManageHandler(bootstrap, chargePileStatusManager, topic,chargeOrderService,channel));
			}
		});
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);
		addr = new InetSocketAddress(host, port);
		bootstrap.setOption("remoteAddress", addr);
		ChannelFuture future = bootstrap.connect(addr);
		if (!future.isSuccess()) {
//			future.getCause().printStackTrace();
			System.out.println(future.isSuccess());
		}
		future.getChannel().getCloseFuture().awaitUninterruptibly();
	} 

}
