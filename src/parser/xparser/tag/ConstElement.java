package parser.xparser.tag;

/**
 * 常量标签
 * 
 * @author ChenSijiang
 * @since 1.0
 */
public class ConstElement extends Tag {

	/*
	 * 使用方法: <const value="" />
	 */

	private String value;

	public ConstElement() {
		super("const");
	}

	public ConstElement(String value) {
		this();
		this.value = value;
	}

	/**
	 * @return 返回字符串 value
	 */
	@Override
	public Object apply(Object params) {
		return value;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}
