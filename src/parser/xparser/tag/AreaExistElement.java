package parser.xparser.tag;

/**
 * 指定区域是否存在
 * 
 * @author ltp Jan 6, 2010
 * @since 1.0
 * @see OwnerElement
 * @see IncludeElement
 */
public class AreaExistElement extends Tag {

	/*
	 * 引用方法: <area-exist start-sign="" end-sign="" />
	 */

	private String startSign; // 开始标记

	private String endSign; // 结束标记

	private boolean startFound; // 是否找到开始标记

	private boolean endFound; // 是否找到结束标记

	public AreaExistElement() {
		super("area-exist");
	}

	public AreaExistElement(String startSign, String endSign) {
		this();
		this.startSign = startSign;
		this.endSign = endSign;
	}

	/**
	 * @param 传入的参数只能是字符串
	 * @return 返回true或者false,true表示包含传入的记录中包含value
	 */
	@Override
	public Object apply(Object params) {
		boolean exist = false;

		if (params != null) {
			String str = params.toString();
			// 如果已经比较过"开始"了,则应该看是找到结束标记
			if (startFound) {
				endFound = exist = str.indexOf(endSign) > -1;
			}
			// 如果还没找到开始标记,则应该首先找开始标记
			else {
				startFound = exist = str.indexOf(startSign) > -1;
			}
		}

		return exist;
	}

	public String getStartSign() {
		return startSign;
	}

	public void setStartSign(String startSign) {
		this.startSign = startSign;
	}

	public String getEndSign() {
		return endSign;
	}

	public void setEndSign(String endSign) {
		this.endSign = endSign;
	}

	public boolean isStartFlag() {
		return startFound;
	}

	public void setStartFlag(boolean startFlag) {
		this.startFound = startFlag;
	}

	public boolean isEndFlag() {
		return endFound;
	}

	public void setEndFlag(boolean endFlag) {
		this.endFound = endFlag;
	}

}
