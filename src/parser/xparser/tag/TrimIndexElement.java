package parser.xparser.tag;

/**
 * 索引截取标记,按索引位置截取字符串
 * 
 * @author ltp Jan 11, 2010
 * @since 1.0
 */
public class TrimIndexElement extends Tag {

	private int start;

	private int end;

	public TrimIndexElement() {
		super("trim-index");
	}

	public TrimIndexElement(int start, int end) {
		this();
		this.start = start;
		this.end = end;
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getEnd() {
		return end;
	}

	public void setEnd(int end) {
		this.end = end;
	}

	/**
	 * 此方法只能接收字符串
	 */
	@Override
	public Object apply(Object params) {
		String str = null;
		if (params != null) {
			String s = params.toString();
			if (end == 0) {
				end = s.length();
			}
			if (start < end) {
				str = s.substring(start, end);
			}
			if (this.hasChild()) {
				Tag[] childs = this.getChild();
				for (Tag child : childs) {
					child.apply(str);
				}
			}
		}
		return str;
	}

	public static void main(String[] args) {
		TrimIndexElement t = new TrimIndexElement(0, 2);
		System.out.println(t.apply("abc"));
	}

}
