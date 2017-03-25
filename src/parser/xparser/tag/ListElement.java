package parser.xparser.tag;

/**
 * 列表引用标记
 * 
 * @author ltp Jan 11, 2010
 * @since 1.0
 * @see ItemElement
 * @see TrimSignElement
 */
public class ListElement extends Tag {

	/*
	 * 引用方法<list>... </list>
	 */

	public ListElement() {
		super("list");
	}

	/**
	 * @param params
	 *            不依赖接收参数
	 * @return String[](item的value数组)
	 */
	@Override
	public Object apply(Object params) {
		if (params == null) {
			return null;
		}
		String[] itemsValue = null;
		Tag[] childs = getChild();
		if (hasChild()) {
			int size = childs.length;
			itemsValue = new String[size];
			for (int i = 0; i < childs.length; i++) {
				ItemElement it = (ItemElement) childs[0];
				itemsValue[i] = it.getValue();
			}
		}
		return itemsValue;
	}

}
