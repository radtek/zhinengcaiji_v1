package parser.xparser.tag;

/**
 * 字符串连接标签
 * 
 * @author YangJian
 * @since 1.0
 * @see ConstElement
 * @see TrimSignElement
 */
public class StrCatElement extends Tag {

	/*
	 * 使用方法: <strcat mark=":"> <const value="head" /> <trim-sign start="" end="" /> </strcat>
	 */

	private String mark = ":"; // 连接符号，默认为冒号

	public StrCatElement() {
		super("strcat");
	}

	public StrCatElement(String mark) {
		this();
		this.mark = mark;
	}

	/**
	 * @param 只接受字符串
	 * @return 返回各个子节点组合起来的字符串，中间连接符号为mark指定的值
	 */
	@Override
	public Object apply(Object params) {
		if (params != null) {
			if (this.hasChild()) {
				StringBuilder sb = new StringBuilder();
				Tag[] childs = getChild();
				int len = childs.length;
				for (int i = 0; i < len - 1; i++) {
					Object ret = childs[i].apply(params);
					if (ret != null) {
						String s = ret.toString();
						sb.append(s).append(mark);
					}
				}

				Object o = childs[len - 1].apply(params);
				if (o != null) {
					sb.append(o.toString());
				} else
					return null;

				return sb.toString();
			}
		}

		return null;
	}

	public String getMark() {
		return mark;
	}

	public void setMark(String mark) {
		this.mark = mark;
	}

}
