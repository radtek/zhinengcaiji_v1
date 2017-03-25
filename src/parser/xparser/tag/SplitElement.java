package parser.xparser.tag;

public class SplitElement extends Tag {

	private String value;

	private int times;

	public SplitElement(String value) {
		super("split");
		this.value = value;
	}

	public SplitElement(String value, int times) {
		super("split");
		this.value = value;
		this.times = times;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int getTimes() {
		return times;
	}

	public void setTimes(int times) {
		this.times = times;
	}

	/**
	 * @param 接受的参数只能是字符串
	 */
	@Override
	public Object apply(Object params) {
		if (params != null) {
			String[] strFields = params.toString().split(value, times);

			if (this.hasChild()) {
				Tag[] childs = this.getChild();
				for (Tag child : childs) {
					child.apply(strFields);
				}
			}

			return strFields;
		}

		return null;
	}

}
