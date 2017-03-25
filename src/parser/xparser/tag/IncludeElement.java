package parser.xparser.tag;

/**
 * 包含标记,是否包含指定的值
 * 
 * @author ltp Jan 6, 2010
 * @since 1.0
 * @see OwnerElement
 * @see AreaExistElement
 */
public class IncludeElement extends Tag {

	/*
	 * 引用方法: <include value="" />
	 */

	private String value;

	public IncludeElement() {
		super("include");
	}

	public IncludeElement(String value) {
		this();
		this.value = value;
	}

	/**
	 * @param 传入的参数只能是字符串
	 * @return 返回true或者false,true表示包含传入的记录中包含value
	 */
	@Override
	public Object apply(Object params) {
		boolean inlcudeFlag = false;
		if (params != null) {
			String str = params.toString();
			inlcudeFlag = str.indexOf(value) > -1;
		}

		return inlcudeFlag;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}
