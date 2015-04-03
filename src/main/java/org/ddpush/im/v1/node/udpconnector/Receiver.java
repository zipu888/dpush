package org.ddpush.im.v1.node.udpconnector;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.ddpush.im.util.DateTimeUtil;
import org.ddpush.im.util.StringUtil;
import org.ddpush.im.v1.node.ClientMessage;

public class Receiver implements Runnable{
	
	protected DatagramChannel channel;
	
	protected int bufferSize = 1024;
	
	protected boolean stoped = false;
	protected ByteBuffer buffer;
	private SocketAddress address;

	protected AtomicLong queueIn = new AtomicLong(0);
	protected AtomicLong queueOut = new AtomicLong(0);

    /**
     * 这里定义一个队列
     */
	protected ConcurrentLinkedQueue<ClientMessage> mq = new ConcurrentLinkedQueue<ClientMessage>();
	
	public Receiver(DatagramChannel channel){
		this.channel = channel;
	}
	
	public void init(){
		buffer = ByteBuffer.allocate(this.bufferSize);
	}
	
	public void stop(){
		this.stoped = true;
	}
	
	public void run(){
		while(!this.stoped){
			try{
				//synchronized(enQueSignal){
					processMessage();
				//	if(mq.isEmpty() == true){
				//		enQueSignal.wait();
				//	}
				//}
			}catch(Exception e){
				e.printStackTrace();
			}catch(Throwable t){
				t.printStackTrace();
			}
		}
	}

    /**
     * 处理消息
     * @throws Exception
     */
	protected void processMessage() throws Exception{
		address = null;
		buffer.clear();
		try{
			address = this.channel.receive(buffer);
		}catch(SocketTimeoutException timeout){
			
		}
		if(address == null){
			try{
				Thread.sleep(1);
			}catch(Exception e){
				
			}
			return;
		}

        /**
         *  如果现在想用这个缓冲区进行信道的写操作，由于write()方法将从position指示的位置开始读取数据，
         *  在limit指示的位置停止，因此在进行写操作前，先要将limit的值设为position的当前值，再将position的值设为0。
         *  这个操作可以通过这个flip()方法实现。
         *  flip()使缓冲区为一系列新的通道写入或相对获取 操作做好准备：它将限制设置为当前位置，然后将位置设置为0，
         *  即上边的要求（红色字体表示）。
         */
		buffer.flip();


		byte[] swap = new byte[buffer.limit() - buffer.position()];

        //拷贝数据

		System.arraycopy(buffer.array(), buffer.position(), swap, 0, swap.length);

        //生成这个消息
		ClientMessage m = new ClientMessage(address,swap);
		
		enqueue(m);
		//System.out.println(DateTimeUtil.getCurDateTime()+" r:"+StringUtil.convert(m.getData())+" from:"+m.getSocketAddress().toString());

	}
	
	protected boolean enqueue(ClientMessage message){
		//加入队列
        boolean result = mq.add(message);
		if(result == true){
			//计数器++
            queueIn.addAndGet(1);
		}
		return result;
	}


	protected ClientMessage dequeue(){
		//从队列中拉出来一个
        ClientMessage m = mq.poll();
		if(m != null){
            //计数器--
			queueOut.addAndGet(1);
		}
		return m;
	}

    /**
     * 接收一个数据
     * @return
     */
	public ClientMessage receive() {

		ClientMessage m = null;
		while(true){
            //出队列
			m = dequeue();
			if(m == null){
				return null;
			}
			if(m.checkFormat() == true){//检查包格式是否合法，为了网络快速响应，在这里检查，不在接收线程检查
				return m;
			}
		}
	}
}
