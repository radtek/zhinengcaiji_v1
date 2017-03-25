package parser.xparser.tag;

/**
 * 参数传递标记(传递什么就返回什么)
 * 
 * @author ChenSijiang
 * @since 1.0
 */
public class RawElement extends Tag {

	/*
	 * 引用说明:
	 * 
	 * <raw />
	 */
	public RawElement() {
		super("raw");
	}

	// 传递什么就返回什么
	@Override
	public Object apply(Object params) {
		return params;
	}

}
