package util;

import java.util.Deque;
import java.util.LinkedList;

/**
 * 消息队列类
 * 
 * @author YangJian
 * @since 3.0
 */
public class MsgQueue<T> {

	private Deque<T> queue;

	// 队列中存放消息的最大条数,默认为5000
	private static final int MAX_COUNT = 5000;

	public MsgQueue() {
		queue = new LinkedList<T>();
	}

	/**
	 * 从队列中取出头部元素
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public synchronized T get() throws InterruptedException {
		// 如果队列中已无消息，则等待
		while (queue.peek() == null) {
			wait();
		}

		notifyAll();

		return queue.poll();
	}

	/**
	 * 把消息加入到队列尾部
	 * 
	 * @param msg
	 * @throws InterruptedException
	 */
	public synchronized void put(T msg) throws InterruptedException {
		if (msg == null)
			return;

		while (queue.size() == MAX_COUNT) {
			wait();
		}

		boolean bReturn = queue.offer(msg);

		// 通知 消息发送线程 发送消息
		if (bReturn)
			notifyAll();
	}

	/**
	 * 把消息加入到队列首部
	 * 
	 * @param msg
	 * @throws InterruptedException
	 */
	public synchronized void putFirst(T msg) throws InterruptedException {
		if (msg == null)
			return;

		while (queue.size() == MAX_COUNT) {
			wait();
		}

		boolean bReturn = queue.offerFirst(msg);

		// 通知 消息发送线程 发送消息
		if (bReturn)
			notifyAll();
	}

	public synchronized void clear() {
		queue.clear();
	}

}
