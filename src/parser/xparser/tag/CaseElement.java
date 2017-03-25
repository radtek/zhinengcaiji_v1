package parser.xparser.tag;

/**
 * case标签
 * 
 * @author YangJian
 * @since 1.0
 * @see SwitchElement
 * @see DefaultElement
 */
public class CaseElement extends Tag {

	/*
	 * 引用方法： <case return=""></case> <case return-ref=""></case>
	 */

	private String returnValue;

	private RuleElement returnRef; // 结果参考规则

	public CaseElement() {
		super("case");
	}

	/**
	 * @param String类型
	 * @return 返回String型,如果返回null，表示case匹配不成功
	 */
	@Override
	public Object apply(Object params) {
		if (params == null) {
			return null;
		}
		String data = params.toString();
		Tag[] childs = getChild();
		if (hasChild()) {
			for (Tag tag : childs) {
				if (!Boolean.parseBoolean(tag.apply(data).toString())) {
					return null;
				}
			}
		}

		Object o = returnValue;
		if (returnRef != null && returnValue == null) {
			o = returnRef.apply(params);
		}

		return o;
	}

	public String getReturnValue() {
		return returnValue;
	}

	public void setReturnValue(String returnValue) {
		this.returnValue = returnValue;
	}

	public RuleElement getReturnRef() {
		return returnRef;
	}

	public void setReturnRef(RuleElement returnRef) {
		this.returnRef = returnRef;
	}

}
