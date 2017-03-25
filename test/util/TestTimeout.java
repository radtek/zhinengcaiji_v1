package util;

import java.util.Date;

public class TestTimeout {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		myThread thread = new TestTimeout().new myThread("me");
		//Thread.currentThread();
		protectThread pthread = new TestTimeout().new protectThread(thread, "protector");
		pthread.start();
		
		thread.start();
	}
	
	public class protectThread extends Thread {
		
		Thread thread = null;
		
		public protectThread(){
			super();
		}
		
		public protectThread(Thread th, String name){
			this.thread = th;
			this.setName(name);
		}
		
		@Override
		public void run(){
			System.out.println(this.getName() + "开始监控线程:" + this.thread.getName() + ",时间：" + new Date());
			try {
				Thread.sleep(2000);//超时2S
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println(this.getName() + "线程超时，被打断");
			}
			if(this.thread != null){
				this.thread.interrupt();
			}
		}
	}
	
	public class myThread extends Thread {
		double count = 0; 
		
		public myThread(String name){
			super(name);
		}
		@Override
		public void run(){
			System.out.println(this.getName() + ":开始运行");
			try{
				while(count<10000000000d){
					int m = 1+2;
					m = m/2;
					count++;
				}
				Thread.sleep(100);
			}catch (InterruptedException ie) {
				System.out.println(this.getName() + "线程超时，被打断");
			}
		}
		
		public void exit(){
			System.out.println(this.getName() + "线程while循环执行了多少次：" + count);
			System.out.println(this.getName() + "线程被销毁时间：" + new Date());
		}
	}

}
