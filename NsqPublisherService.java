package com.cetcnav.lbs.weixin.service;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.nsqjava.core.NSQChannelHandler;
import org.nsqjava.core.NSQFrameDecoder;
import org.nsqjava.core.commands.Publish;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cetcnav.lbs.weixin.protocolbean.ManageProtocol;
import com.cetcnav.lbs.weixin.protocolbean.ManageProtocol.Command;
import com.cetcnav.lbs.weixin.protocolbean.ManageProtocol.Command.CommandType;
import com.cetcnav.lbs.weixin.protocolbean.ParamProtocol;
import com.cetcnav.lbs.weixin.protocolbean.ParamProtocol.Param;


/**
 * nsq发布服务类
 * 
 * @author linlj
 *
 */
public class NsqPublisherService {

	private static final Logger log = LoggerFactory.getLogger(NsqPublisherService.class);

	private String host;
	private int port;
	private Channel chan;
	private ClientBootstrap bootstrap;
	private NSQChannelHandler nsqhndl;
	private ChannelFactory factory;
	private String uuid;
	
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

	public Channel getChan() {
		return chan;
	}

	public void setChan(Channel chan) {
		this.chan = chan;
	}
	

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	@PostConstruct  
    public void  init(){  
	   factory = new NioClientSocketChannelFactory();

		bootstrap = new ClientBootstrap(factory);
		nsqhndl = new NSQChannelHandler(bootstrap);

		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

			public ChannelPipeline getPipeline() {
				return Channels.pipeline(new NSQFrameDecoder(), nsqhndl);
			}
		});
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);
		bootstrap.setOption("remoteAddress", new InetSocketAddress(this.host, this.port));
		ChannelFuture future = bootstrap.connect();

		future.awaitUninterruptibly();
		if (!future.isSuccess()) {
			future.getCause().printStackTrace();
			return;
		}
		this.chan = future.getChannel();
   }  
	public void destory(){
		nsqhndl.close().awaitUninterruptibly();
		bootstrap.releaseExternalResources();
		factory.releaseExternalResources();
	}
	/**
	 * 发布方法
	 * @param topic
	 * @param byteArrayInfos
	 */
    public void publisher(String topic, byte[] byteArrayInfos){
    	Publish pub = new Publish(topic, byteArrayInfos);
    	if(!chan.isOpen()){
    		destory();
    		init();
    	}
		chan.write(pub);
    }
    
    /**
     * 初始化更新充电桩的请求：准备开始充电，获取充电桩状态
     */
    public Command initPreRechargeReqCommand(long cpid,int timestamp,int pcode){
		ManageProtocol.Command.Builder command = 
				ManageProtocol.Command.newBuilder();
//		command.setType(CommandType.CMT_REQ_CHARGING_PREPARE);//请求充电(充电前获取充电桩状态)请求
		command.setUuid(uuid);
		command.setTid(cpid);
		command.setSerialNumber(timestamp);
		List<Param> lstParam = new ArrayList<Param>();
		ParamProtocol.Param.Builder paramBuilder = ParamProtocol.Param.newBuilder();
		paramBuilder.setNpara(pcode);
		lstParam.add(paramBuilder.build());
		command.addAllParas(lstParam);
    	return command.build();
    }
    /**
     * 发布开始充电的请求：开始充电
     */
    public void pubPreRechargeReqCommand(long cpid,int timestamp,int pCode,String topic){
    	log.info("Command:"+CommandType.CMT_REQ_CHARGING+";cpid:"+cpid+";timestamp:"+timestamp+";pCode:"+pCode+";topic:"+topic);
    	Command command = initPreRechargeReqCommand(cpid,timestamp,pCode);
    	publisher(topic, command.toByteArray());
    }
    
    /**
     * 初始化 开始充电 的请求，通知充电桩开始充电
     */
    public Command initRechargeReqCommand(long cpid,int timestamp,String openid,int pcode,String orderNumber, int totalFee){
		ManageProtocol.Command.Builder command = 
				ManageProtocol.Command.newBuilder();
		command.setType(CommandType.CMT_REQ_CHARGING);//请求开始充电
		command.setUuid(uuid);
		command.setTid(cpid);
		command.setSerialNumber(timestamp);
		List<Param> lstParam = new ArrayList<Param>();
		ParamProtocol.Param.Builder paramBuilder = ParamProtocol.Param.newBuilder();
		paramBuilder.setStrpara(openid);
		lstParam.add(paramBuilder.build());
		paramBuilder = ParamProtocol.Param.newBuilder();
		paramBuilder.setStrpara(pcode+"");
		lstParam.add(paramBuilder.build());
		
		paramBuilder = ParamProtocol.Param.newBuilder();
		paramBuilder.setStrpara(orderNumber);
		lstParam.add(paramBuilder.build());
		paramBuilder = ParamProtocol.Param.newBuilder();
		paramBuilder.setNpara(totalFee);
		lstParam.add(paramBuilder.build());
		command.addAllParas(lstParam);
    	return command.build();
    }
    
    /**
     * 发布开始充电的请求：开始充电，获取订单的一些状态
     */
    public void pubRechargeReqCommand(long cpid,int timestamp,String openid,
    		int pcode,String topic,String orderNumber, int totalFee){
    	log.info("Command:"+CommandType.CMT_REQ_CHARGING+";cpid:"+cpid+";timestamp:"+timestamp+";totalFee:"+totalFee+";topic:"+topic+";orderNumber:"+orderNumber);
    	
    	Command command = initRechargeReqCommand(cpid,timestamp,openid,pcode,orderNumber,totalFee);
    	publisher(topic, command.toByteArray());
    }
    
    /**
     * 初始化 结束充电 的请求，通知充电桩开始充电
     */
    public Command initEndRechargeReqCommand(long cpid,int timestamp,String openid,String orderNumber){
		ManageProtocol.Command.Builder command = 
				ManageProtocol.Command.newBuilder();
		command.setType(CommandType.CMT_REQ_STOP_CHARGING);//请求结束充电
		command.setUuid(uuid);
		command.setTid(cpid);
		command.setSerialNumber(timestamp);
		List<Param> lstParam = new ArrayList<Param>();
		ParamProtocol.Param.Builder paramBuilder = ParamProtocol.Param.newBuilder();
		paramBuilder.setStrpara(openid);
		lstParam.add(paramBuilder.build());
		paramBuilder = ParamProtocol.Param.newBuilder();
		paramBuilder.setStrpara(orderNumber);
		lstParam.add(paramBuilder.build());
		command.addAllParas(lstParam);
    	return command.build();
    }   
    /**
     * 发布结束充电的请求：通知充电桩结束充电
     */
    public void pubEndRechargeReqCommand(long cpid,int timestamp,String openid,int pcode,String topic,String orderNumber, int totalFee){
    	Command command = initEndRechargeReqCommand(cpid,timestamp,openid,orderNumber);
    	log.info("Command:"+CommandType.CMT_REQ_STOP_CHARGING+"topic:"+topic+";cpid:"+cpid+";timestamp:"+timestamp+";openid:"+openid+";orderNumber:"+orderNumber);
    	publisher(topic, command.toByteArray());
    }

    
    
    /**
     * 初始化平台请求枪头状态
     */
    public Command initGetGunStatusReqCommand(long cpid,int timestamp){
		ManageProtocol.Command.Builder command = 
				ManageProtocol.Command.newBuilder();
		command.setType(CommandType.CMT_REQ_GET_GUN_STATUS);//平台请求枪头状态
		command.setUuid(uuid);// 发起消息的服务器id
		command.setTid(cpid);// 充电桩号
		command.setSerialNumber(timestamp); // 序列号
    	return command.build();
    }   
    /**
     * 发布平台请求枪头状态的请求
     */
    public void pubGetGunStatusReqCommand(long cpid,int timestamp,String topic){
    	Command command = initGetGunStatusReqCommand(cpid,timestamp);
    	log.info("Command:"+CommandType.CMT_REQ_GET_GUN_STATUS_VALUE+"topic:"+topic+";cpid:"+cpid+";timestamp:"+timestamp);
    	publisher(topic, command.toByteArray());
    }
}
