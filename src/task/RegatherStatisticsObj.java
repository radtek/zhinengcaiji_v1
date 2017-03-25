package task;

/**
 * 补采任务补采情况统计对象
 * 
 * @author liuwx 2010-4-21
 * @since 3.0
 */
public class RegatherStatisticsObj {

	private long key; // crc32(补采任务的TASKID+FILEPATH+COLLECTTIME)

	private int times; // 补采次数

	public RegatherStatisticsObj() {
		super();
	}

	public RegatherStatisticsObj(long key, int times) {
		super();
		this.key = key;
		this.times = times;
	}

	public long getKey() {
		return key;
	}

	public void setKey(long key) {
		this.key = key;
	}

	public int getTimes() {
		return times;
	}

	public void setTimes(int times) {
		this.times = times;
	}

}
