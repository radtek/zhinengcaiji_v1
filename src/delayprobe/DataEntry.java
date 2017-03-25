package delayprobe;

/**
 * 厂商设备上的一个数据单元
 * 
 * @author ChenSijiang 2010-08-03
 * @version 1.1
 */
public class DataEntry {

	private String name;

	private long size;

	public DataEntry() {
		super();
	}

	public DataEntry(String name, long size) {
		super();
		this.name = name;
		this.size = size;
	}

	public DataEntry(String name) {
		super();
		this.name = name;
	}

	/**
	 * 获取该数据单元的名称。
	 * 
	 * @return 该数据单元的名称。
	 */
	public String getName() {
		return name;
	}

	/**
	 * 获取该数据单元的大小，-1表示该数据单元不存在。
	 * 
	 * @return 该数据单元的大小，-1表示该数据单元不存在。
	 */
	public long getSize() {
		return size;
	}

	/**
	 * 重新设置该数据单元的大小
	 * 
	 * @param size
	 *            该数据单元新的大小
	 */
	public void setSize(long size) {
		this.size = size;
	}

	@Override
	public boolean equals(Object obj) {
		DataEntry e = (DataEntry) obj;
		return getName().equals(e.getName());
	}
	
	@Override
	public int hashCode(){
		return this.getName().hashCode();
	}

	@Override
	public String toString() {
		return "[文件名:" + name + "  大小:" + size + "]";
	}
}
