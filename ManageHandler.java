package com.cetcnav.lbs.weixin.handler;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Queue;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.nsqjava.core.NSQChannelHandler;
import org.nsqjava.core.NSQFrame;
import org.nsqjava.core.commands.Finish;
import org.nsqjava.core.commands.Ready;
import org.nsqjava.core.commands.Subscribe;
import org.nsqjava.core.enums.ResponseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cetcnav.lbs.base.util.CommonUtils;
import com.cetcnav.lbs.weixin.model.ChargeOrder;
import com.cetcnav.lbs.weixin.model.enums.ChargeOrderStatusEnum;
import com.cetcnav.lbs.weixin.protocolbean.ManageProtocol.Command;
import com.cetcnav.lbs.weixin.protocolbean.ManageProtocol.Command.CommandType;
import com.cetcnav.lbs.weixin.protocolbean.ParamProtocol.Param;
import com.cetcnav.lbs.weixin.service.ChargeOrderService;
import com.cetcnav.lbs.weixin.service.ChargePileStatusManager;
import com.cetcnav.lbs.weixin.util.WXUtils;


public class ManageHandler extends NSQChannelHandler {
	private static final Logger log = LoggerFactory.getLogger(ManageHandler.class);

	/* 充电完成的订单 */
	private Queue<ChargeOrder> chargeOrderFinished;
	private ChargePileStatusManager chargePileStatusManager;
	private ChargeOrderService chargeOrderService;
	String topic = null;
	String channel = "";
	public Queue<ChargeOrder> getChargeOrderFinished() {
		return chargeOrderFinished;
	}

	public ManageHandler(ClientBootstrap bs, ChargePileStatusManager chargePileStatusManager, String topic,ChargeOrderService chargeOrderService, String channel) {
		super(bs);
		this.topic = topic;
		this.channel = channel;
		this.chargePileStatusManager = chargePileStatusManager;
		this.chargeOrderService = chargeOrderService;
		ChargeOrderStatHandler handler = ChargeOrderStatHandler.getChargeOrderStatHandler(chargeOrderService);
		chargeOrderFinished = handler.getChargeOrderFinishedQueue();
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		System.out.println("Received message " + e.getMessage());
		Object o = e.getMessage();
		if (o instanceof NSQFrame) {
			System.out.println("received nsqframe");
			NSQFrame frm = (NSQFrame) o;
			try {
				System.out.println("Received message\n" + new String(frm.getMsg().getBody()));
				 Command command = com.cetcnav.lbs.weixin.protocolbean.ManageProtocol.Command.parseFrom(frm.getMsg().getBody());
				// do stuff with it.
				System.out.println("指令名称"+command.getType() );
				long cpid = command.getTid();
				List<Param> lstParam = command.getParasList();
				switch(command.getTypeValue()){
				case CommandType.CMT_REP_CHARGING_VALUE:
					System.out.println("平台请求开始充电结果");
					if(lstParam.size()>=2){
						Param paramPcode = lstParam.get(0);
						int result = (int) paramPcode.getNpara();
						paramPcode = lstParam.get(1);
						String  orderNumber = (String) paramPcode.getStrpara();
						orderNumber = orderNumber.trim();
						System.out.println("cpid:"+cpid+";result:"+result+";orderNumber:"+orderNumber);
						if(StringUtils.isNotBlank(orderNumber)){
							if(result!=0){
								ChargeOrder chargeOrder = chargeOrderService.getChargeOrderByOrderNumber(orderNumber);
								if(chargeOrder!=null&&ChargeOrderStatusEnum.PREPAY_SUCCESS.getStatus()==chargeOrder.getStatus()){
									chargeOrder.setStatus(ChargeOrderStatusEnum.NOT_CHARGE.getStatus());
									chargeOrder.setRemark("充电桩异常不能充电，进行自动退款");
									chargeOrder.setRefund(chargeOrder.getPrepayment());
									DecimalFormat    df   = new DecimalFormat("0000000000"); 
									String outTradeNo = CommonUtils.getCurrentDate(CommonUtils.DEFAULT_DATETIMEFORORDER_FORMAT)+chargeOrder.getOrderNumber().substring(0, 8)+df.format(chargeOrder.getId());
									chargeOrder.setRefundOrderNumber(outTradeNo);
									chargeOrderService.updateChargeOrder(chargeOrder);
									System.out.println("订单："+orderNumber+"不能充电，进行退款");
									chargeOrderService.refundOrder(chargeOrder,ChargeOrderStatusEnum.SETTLED.getStatus());
								}
							}else{
								ChargeOrder chargeOrder = chargeOrderService.getChargeOrderByOrderNumber(orderNumber);
								if(chargeOrder!=null){
									chargeOrder.setStatus(ChargeOrderStatusEnum.WAIT_CHARGE.getStatus());
									chargeOrderService.updateChargeOrder(chargeOrder);
								}
							}
						}
					}
					break;
				case CommandType.CMT_REP_CHARGING_STARTED_VALUE:

					System.out.println("充电桩已经开始的充电");
//					//将充电桩状态放入线程安全的Map中
					if(lstParam.size()>=2){
						Param paramPcode = lstParam.get(0);
						String wxUserId = paramPcode.getStrpara();
						paramPcode = lstParam.get(1);
						String orderNumber = paramPcode.getStrpara();
						orderNumber = orderNumber.trim();
						System.out.println("充电开始-orderNumber:"+orderNumber+";wxUserId:"+wxUserId);
						ChargeOrder  chargeOrder = chargeOrderService.getChargeOrderByOrderNumber(orderNumber);
						if(chargeOrder!=null){
							if(ChargeOrderStatusEnum.WAIT_CHARGE.getStatus() == chargeOrder.getStatus()){
								chargeOrder.setStatus(ChargeOrderStatusEnum.CHARGING.getStatus());
								chargeOrderService.updateChargeOrder(chargeOrder);
								//将内存中充电桩状态置为开始充电避免 再次进行充值付款
								chargePileStatusManager.addChargePileStatus(WXUtils.getCpidFormat(cpid), ChargeOrderStatusEnum.CHARGING.getStatus());
							}
						}
					}
					break;
				case CommandType.CMT_REP_STOP_CHARGING_VALUE:
					System.out.println("充电桩反馈接到结束充电信号");
					if(lstParam.size()>=1){
						Param param = lstParam.get(0);
						int result = (int) param.getNpara();
						System.out.println("充电桩反馈接到结束充电信号result:"+result);
					}
					break;
				case CommandType.CMT_NOTIFY_TRANSCATION_VALUE:
					chargePileStatusManager.delChargePileStatus(WXUtils.getCpidFormat(cpid));
					System.out.println("充电桩结束充电");
					if(lstParam.size()>=1){
						Param param = lstParam.get(0);
						String strOrderNumbers = (String) param.getStrpara();
						System.out.println("cpid:"+cpid+";strOrderNumbers:"+strOrderNumbers);
						String[] arrayOrderNumbers = strOrderNumbers.split(",");
						System.out.println("arrayOrderNumbers.length="+arrayOrderNumbers.length);
						for(int i = arrayOrderNumbers.length; i > 0; i--){
							arrayOrderNumbers[i-1] = arrayOrderNumbers[i-1].trim();
						}
						Arrays.sort(arrayOrderNumbers);
						if(arrayOrderNumbers.length <= 0 || StringUtils.isBlank(arrayOrderNumbers[0]) || arrayOrderNumbers[0].length()<8 || 
								StringUtils.isBlank(arrayOrderNumbers[arrayOrderNumbers.length-1])||arrayOrderNumbers[arrayOrderNumbers.length-1].length()<8){
							break;
						}
						Date beginDate = CommonUtils.stringToDate(arrayOrderNumbers[0].substring(0,8), "yyyyMMdd");
						Date endDate = CommonUtils.stringToDate(arrayOrderNumbers[arrayOrderNumbers.length-1].substring(0,8), "yyyyMMdd");
						Calendar   calendar   =   new   GregorianCalendar(); 
					    calendar.setTime(endDate); 
					    calendar.add(calendar.DATE,1);//把日期往后增加一天.整数往后推,负数往前移动 
					    endDate=calendar.getTime();
						List<ChargeOrder> lstChargeOrder = chargeOrderService.getChargePileOrder(beginDate,endDate,arrayOrderNumbers);
						if(lstChargeOrder!=null&&lstChargeOrder.size()>0){
							System.out.println("lstChargeOrder.size:"+lstChargeOrder.size());
							for(ChargeOrder  chargeOrder :lstChargeOrder){
								System.out.println("chargeOrder.status:"+chargeOrder.getStatus());
								if(chargeOrder!=null&&chargeOrder.getStatus()==ChargeOrderStatusEnum.END_CHARGE.getStatus()){
									chargeOrder.setStatus(ChargeOrderStatusEnum.REFUNDING.getStatus());//先将结果变为退款状态，防止再次退款。
									chargeOrder.setRefund(0.0);
									chargeOrderService.updateChargeOrder(chargeOrder);
									System.out.println("chargeOrder.getMoney():"+chargeOrder.getMoney());
									if(chargeOrder.getMoney()<chargeOrder.getPrepayment()){
										BigDecimal b1 = new BigDecimal(chargeOrder.getPrepayment().toString());

										BigDecimal b2 = new BigDecimal(chargeOrder.getMoney().toString());

										double refundMoney = b1.doubleValue();
										if(chargeOrder.getMoney()>=0){
											refundMoney = b1.subtract(b2).doubleValue();
										}
										System.out.println("Refund:"+refundMoney);
										chargeOrder.setStatus(ChargeOrderStatusEnum.REFUNDING.getStatus());//申请退款
										DecimalFormat    df   = new DecimalFormat("0000000000"); 
										String outTradeNo = CommonUtils.getCurrentDate(CommonUtils.DEFAULT_DATETIMEFORORDER_FORMAT)+chargeOrder.getOrderNumber().substring(0, 8)+df.format(chargeOrder.getId());
										chargeOrder.setRefundOrderNumber(outTradeNo);
										chargeOrder.setRefund(refundMoney);
										chargeOrderService.updateChargeOrder(chargeOrder);
										chargeOrderService.refundOrder(chargeOrder);
									}else{
										chargeOrder.setStatus(ChargeOrderStatusEnum.REFUNDED.getStatus());
										chargeOrderService.updateChargeOrder(chargeOrder);
									}
									//郭昊加入报表数据
									if(chargeOrder.getStatus()==ChargeOrderStatusEnum.REFUNDED.getStatus()){
										chargeOrderFinished.offer(chargeOrder);
									}
								}
							}
							
						}else{
							System.out.println("订单数量为空");
						}
					}
					break;
				case CommandType.CMT_REP_GET_GUN_STATUS_VALUE:
					System.out.println("获取枪头状态");
					//将充电桩状态放入线程安全的Map中
					if(lstParam.size()>=1){
						Param param = lstParam.get(0);
						int result = (int) param.getNpara();
						System.out.println("cpid:"+cpid+";result:"+result);
						chargePileStatusManager.addChargePileStatus(WXUtils.getCpidFormat(cpid), result);
					}
					break;
					
				}
				// once done, confirm with server
				System.out.println("Confirming message\n" + new String(frm.getMsg().getMessageId()));
				ChannelFuture future = e.getChannel().write(new Finish(frm.getMsg().getMessageId()));
				future.sync();
				System.out.println("RDY again");
				e.getChannel().write(new Ready(100)).sync();
			} catch (Exception ex) {
				log.error("Failed to process message due to exception", ex);
				// failed to process message, requeue
//				e.getChannel().write(new Requeue(frm.getMsg().getMessageId(), 1));
			}
		} else if (o instanceof ResponseType) {
			// don't care
		} else {
			//
		}
	}

	@Override
	protected void nsqAuthenticated(ChannelFuture future) {
		Channel chan = future.getChannel();
		Subscribe sub = new Subscribe(topic, channel, "SUBSCRIBERSHORT", "SUBSCRIBERLONG");
		System.out.println("Subscribing to " + sub.getCommandString());
		chan.write(sub);
		Ready rdy = new Ready(100);
		chan.write(rdy);

	}

}
