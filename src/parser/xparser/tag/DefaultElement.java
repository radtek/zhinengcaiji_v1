package parser.xparser.tag;

public class DefaultElement extends Tag {

	private String returnValue;

	public DefaultElement() {
		super("default");
	}

	/**
	 * 如果returnValue为空字符串的话，将返回null
	 * 
	 * @param String类型
	 * @return 返回String型
	 */
	@Override
	public Object apply(Object params) {
		if (returnValue != null && returnValue.trim().length() == 0) {
			return null;
		}
		return returnValue;
	}

	public String getReturnValue() {
		return returnValue;
	}

	public void setReturnValue(String returnValue) {
		this.returnValue = returnValue;
	}

}
