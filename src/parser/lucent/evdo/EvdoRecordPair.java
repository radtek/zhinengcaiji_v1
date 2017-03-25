package parser.lucent.evdo;

/**
 * Evdo话单中的一对记录。
 * 
 * @author ChenSijiang 2009.01.26
 * @since 1.0
 */
public class EvdoRecordPair {

	String name;

	String value;

	/**
	 * 构造方法，指定记录名与值
	 * 
	 * @param name
	 *            记录名
	 * @param value
	 *            记录值
	 */
	public EvdoRecordPair(String name, String value) {
		super();
		this.name = name;
		this.value = value;
	}

	/**
	 * 获取记录名
	 * 
	 * @return 记录名
	 */
	public String getName() {
		return name;
	}

	/**
	 * 设置记录名
	 * 
	 * @param name
	 *            记录名
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * 获取记录值
	 * 
	 * @return 记录值
	 */
	public String getValue() {
		return value;
	}

	/**
	 * 设置记录值
	 * 
	 * @param value
	 *            记录值
	 */
	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return String.format("{name:%s value:%s}", name, value);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof EvdoRecordPair) {
			EvdoRecordPair p = (EvdoRecordPair) obj;
			return p.getName().equals(name);
		}
		return false;
	}
	
	@Override
	public int hashCode(){
		return this.name.hashCode();
	}

}
