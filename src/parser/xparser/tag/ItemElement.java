package parser.xparser.tag;

/**
 * 选项标记,用于ListElement的元素
 * 
 * @author ltp Jan 11, 2010
 * @since 1.0
 * @see ListElement
 */
public class ItemElement extends Tag {

	/*
	 * 引用方法 <item value="" />
	 */
	private String value;

	public ItemElement() {
		super("item");
	}

	public ItemElement(String value) {
		this();
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public Object apply(Object params) {
		return value;
	}
}
