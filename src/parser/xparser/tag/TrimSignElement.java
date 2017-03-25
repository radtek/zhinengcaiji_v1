package parser.xparser.tag;

import java.util.ArrayList;
import java.util.List;

/**
 * 符号截取标记,截取符号之间的字符串
 * 
 * @author ltp Jan 11, 2010
 * @since 1.0
 * @see ListElement
 */
public class TrimSignElement extends Tag {

	/*
	 * 引用方法:<trim-sign start="" end="," end-ref="1" >... </trim-sign>
	 */
	private String start;

	private String end;

	private ListElement endRef;

	private boolean greedFlag = false; // 贪婪标记,默认为false,true为开启字符匹配贪婪模式

	public TrimSignElement() {
		super("trim-sign");
	}

	public TrimSignElement(String start, ListElement endRef) {
		this(start);
		this.endRef = endRef;
	}

	public TrimSignElement(String start, String end) {
		this(start);
		this.end = end;
	}

	public TrimSignElement(String start) {
		this();
		this.start = start;
	}

	public ListElement getEndRef() {
		return endRef;
	}

	public void setEndRef(ListElement endRef) {
		this.endRef = endRef;
	}

	public String getStart() {
		return start;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getEnd() {
		return end;
	}

	public void setEnd(String end) {
		this.end = end;
	}

	public boolean isGreedFlag() {
		return greedFlag;
	}

	public void setGreedFlag(boolean greedFlag) {
		this.greedFlag = greedFlag;
	}

	/**
	 * 截取标记符号之间的内容
	 * 
	 * @param 只能接收字符串
	 * @return 返回字符串
	 */
	@Override
	public Object apply(Object params) {
		byte greededTimes = 0; // 被贪婪的次数

		String str = null;
		if (params != null) {
			String s = params.toString();
			// 开始位置
			int beginIndex = 0;
			// 结束位置,因为结束的位置可能会有多个,所以用(list)
			List<Integer> endIndexes = new ArrayList<Integer>();
			// 如果开始标记为空,那么开始位置就为0;否则就为指定标记的位置
			if (start != null) {
				beginIndex = s.indexOf(start);
				if (beginIndex == -1) {
					if (greedFlag) {
						beginIndex = 0;
						greededTimes++;
					}
				} else
					beginIndex += start.length();
			}
			// 如果结束标记end和end_ref都为空,那么结束位置就为传入字符串的最后位置;否则就为指定标记的位置
			if (end == null && endRef == null)// n/n
			{
				endIndexes.add(s.length());
			} else if (end != null && endRef == null)// b/n
			{
				int endPos = s.indexOf(end, beginIndex);
				if (endPos == -1) {
					if (greedFlag) {
						endPos = s.length();
						endIndexes.add(endPos);
						greededTimes++;
					}
				} else
					endIndexes.add(endPos);
			} else
			// 新加的方法b/b,n/b
			{
				if (end != null) {
					endIndexes.add(s.indexOf(end, beginIndex));
				}
				String[] itemsValue = (String[]) endRef.apply(null);
				int len = 0;
				if (itemsValue != null && (len = itemsValue.length) != 0) {
					for (int i = 0; i < len; i++) {
						if (itemsValue[i] == null)// 如果为空就表示此字符串的长度
						{
							endIndexes.add(s.length());
						} else {
							endIndexes.add(s.indexOf(itemsValue[i], beginIndex));
						}
					}
				}

			}

			if (greedFlag && greededTimes == 2) {
				return null;
			}

			// 根据开始的结束位置截取字符串
			if (endIndexes.size() != 0) {
				for (Integer endIndex : endIndexes) {
					if (beginIndex != -1 && endIndex != -1 && beginIndex < endIndex) {
						str = s.substring(beginIndex, endIndex);
						if (str != null) {
							if (this.hasChild()) {
								Tag[] tags = this.getChild();
								for (Tag child : tags) {
									child.apply(str);
								}
							}
							break;
						}
					}
				}
			}
		}
		return str;
	}

	public static void main(String[] args) {
		TrimSignElement t = new TrimSignElement("sdf=", "dsfasdf=");
		t.setGreedFlag(true);
		System.out.println(t.apply("MANAGED_OBJECT_INSTANCE=NodeId=135202[dtdxBSC2],BssId=0[大同电信2],BtsId=69[大同县聚乐],RackId=1,SlotId=10,ShelfId=5"));
	}
}
