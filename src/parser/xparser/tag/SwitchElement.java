package parser.xparser.tag;

/**
 * 表示switch标签
 * 
 * @author ChenSijiang
 * @since 1.0
 * @see CaseElement
 * @see DefaultElement
 * */
public class SwitchElement extends Tag {

	/*
	 * 使用方法: <switch> <case return=""> <include value="alarm" /> </case> <case return=""> <area-exist start-sign="" end-sign="" /> </case> <default
	 * return=""> </default> </switch>
	 */
	public SwitchElement() {
		super("switch");
	}

	/**
	 * @param String
	 * @return 返回的case或default的返回值
	 */
	@Override
	public Object apply(Object params) {
		if (params != null) {
			if (hasChild()) {
				Tag[] childs = getChild();
				for (Tag tag : childs) {
					Object ret = tag.apply(params);
					if (ret != null) {
						return ret;
					}
				}
			}
		}
		return null;
	}

}
