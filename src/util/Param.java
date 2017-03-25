package util;

/**
 * 参数 类
 * 
 * @author YangJian
 * @since 3.0
 */
public class Param {

	private String name;

	private String value;

	public Param() {
		super();
	}

	public Param(String name, String value) {
		super();
		this.name = name;
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}
