package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import parser.xparser.tag.SplitElement;

/**
 * <p>
 * 逻辑分隔标签，用于特殊的数据分隔。
 * </p>
 * <p>
 * 例如：1,2,"1,2,3",4,3 <br />
 * 这样的数据，如果用逗号分隔，结果肯定不正确，因为双引号中的内容，<br />
 * 虽然也有逗号，但是它们是一个整体，也就是一列数据。所以，使用此逻辑分隔标签， 用于处理此类特殊的数据。
 * </p>
 * 
 * @author ChenSijiang
 * @since 1.0
 * @see SplitElement
 */
public class LogicSpliter {

	// 分隔符
	private Character value;

	// 用户在配置文件中配置的开始结束符
	private String wrapSigns;

	// 是否保留开始结束符
	private boolean keepSign;

	// 默认的开始结束符 <开始符，结束符>
	private static final Map<Character, Character> DEFAULT_WRAP_SIGNS = new HashMap<Character, Character>();

	// 用户指定的开始结束符 <开始符，结束符>
	private static final Map<Character, Character> USER_WRAP_SIGNS = new HashMap<Character, Character>();

	// 当前要使用的开始结束符。如果用户未指定，则使用DEFAULT_WRAP_SIGNS中的，否则使用用户指定的。 <开始符，结束符>
	Map<Character, Character> signs;

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	/* 放入默认的开始结束符 */
	static {
		DEFAULT_WRAP_SIGNS.put('"', '"');
		DEFAULT_WRAP_SIGNS.put('\'', '\'');
		// DEFAULT_WRAP_SIGNS.put('(', ')');
		// DEFAULT_WRAP_SIGNS.put('{', '}');
		// DEFAULT_WRAP_SIGNS.put('[', ']');
		// DEFAULT_WRAP_SIGNS.put('<', '>');
		DEFAULT_WRAP_SIGNS.put('|', '|');
	}

	// constructors..

	/**
	 * 构造方法，未指定分隔符、开始结束符、是否保留开始结束符，所以这三项指标都将是默认值。<br />
	 * 其中：分隔符必须通过相应setter方法指定，开始结束符使用默认的，是否保留开始结束符为false.
	 * 
	 * @see #LogicSpliter(Character)
	 * @see #setValue(Character)
	 */
	public LogicSpliter() {
		this(null, null, false);
	}

	/**
	 * 构造方法，未指定开始结束符、是否保留开始结束符，所以这两项指标都将是默认值。<br />
	 * 其中：开始结束符使用默认的，是否保留开始结束符为false.
	 * 
	 * @see #LogicSpliter(Character, String)
	 * @param value
	 *            分隔符
	 */
	public LogicSpliter(Character value) {
		this(value, null, false);
	}

	/**
	 * 构造方法，未指定是否保留开始结束符，所以是否保留开始结束符为false，可通过相应setter方法指定。
	 * 
	 * @see #setKeepSign(boolean)
	 * @see #LogicSpliter(Character, String, boolean)
	 * @param value
	 *            分隔符
	 * @param wrapSigns
	 *            开始结束符
	 */
	public LogicSpliter(Character value, String wrapSigns) {
		this(value, wrapSigns, false);
	}

	/**
	 * 构造方法，指定分隔符、开始结束符、是否保留开始结束符。
	 * 
	 * @param value
	 *            分隔符
	 * @param wrapSigns
	 *            开始结束符
	 * @param keepSign
	 *            是否保留开始结束符
	 */
	public LogicSpliter(Character value, String wrapSigns, boolean keepSign) {
		this.value = value;
		this.wrapSigns = wrapSigns;
		this.keepSign = keepSign;
		if (wrapSigns != null) {
			resolveWrapSigns();
		}
	}

	// getters & setters..
	/**
	 * 获取分隔符
	 * 
	 * @return 分隔符
	 */
	public Character getValue() {
		return value;
	}

	/**
	 * 设置分隔符
	 * 
	 * @param value
	 *            分隔符
	 */
	public void setValue(Character value) {
		this.value = value;
	}

	/**
	 * 获取开始结束符
	 * 
	 * @return 开始结束符
	 */
	public String getWrapSigns() {
		return wrapSigns;
	}

	/**
	 * 设置开始结束符
	 * 
	 * @param wrapSigns
	 *            开始结束符
	 */
	public void setWrapSigns(String wrapSigns) {
		this.wrapSigns = wrapSigns;
	}

	/**
	 * 获取是否保留开始结束符
	 * 
	 * @return 是否保留开始结束符
	 */
	public boolean isKeepSign() {
		return keepSign;
	}

	/**
	 * 设置是否保留开始结束符
	 * 
	 * @param keepSign
	 *            是否保留开始结束符
	 */
	public void setKeepSign(boolean keepSign) {
		this.keepSign = keepSign;
	}

	public String[] apply(String str) {
		if (str == null) {
			logger.warn("参数为null");
			return null;
		}

		// 在栈里放的元素，保存开始字符及开始字符所在的位置
		class StackElement {

			char sign;

			int index;

			StackElement(char c, int i) {
				sign = c;
				index = i;
			}

			@Override
			public String toString() {
				return String.format("[sign: %s index: %s]", sign, index);
			}
		}

		// 确定当前是要使用默认开始结束符，还是使用用户指定的开始结束符
		signs = USER_WRAP_SIGNS.size() == 0 ? DEFAULT_WRAP_SIGNS : USER_WRAP_SIGNS;
		// 栈，用于匹配开始结束符
		Stack<StackElement> signStack = new Stack<StackElement>();
		// 将传入的字符串转为char[]，然后逐一扫描
		char[] chars = ("uway" + value + str.toString()).toCharArray();
		// 存放已经好的字段
		List<String> resultList = new ArrayList<String>();
		// 临时字符串，存放单个字段，一旦确定此字段已完，则转入resultList，并清空此buffer
		StringBuilder tempBuffer = new StringBuilder();
		// start与end:保存当前扫描到的开始符与结束符的位置
		int start = 0;
		int end = 0;
		for (int i = 0; i < chars.length; i++) {
			char currentChar = chars[i];
			// 当前字符是类似''这种，开始结束一样的开始结束符
			if (isSameStartEnd(currentChar)) {
				tempBuffer.append(currentChar);
			}
			// 当前字符是开始符
			else if (isStart(currentChar)) {
				StackElement startSignElement = new StackElement(currentChar, i);
				signStack.push(startSignElement);
				start = i;
			}
			// 当前字符是结束符
			else if (isEnd(currentChar) && signStack.size() > 0) {
				StackElement top = signStack.peek();
				if (signs.get(top.sign) == currentChar) // 检查目前栈顶的开始符是否是当前字符的另一半
				{
					end = i;
					signStack.pop();
					if (signStack.size() == 0) {
						resultList.add(subCharArray(chars, top.index, end));
						start = 0;
					}
					end = 0;
				}
			}
			// 不是开始符，也不是结束符，并且不在开始与结束符之间
			else if (start == 0 && end == 0) {
				char preChar = chars[i > 0 ? i - 1 : 0];
				// 当前字符是分隔符
				if (currentChar == value) {
					if (!isStart(preChar) && !isEnd(preChar) || isSameStartEnd(preChar)) {
						resultList.add(tempBuffer.toString());
					}
					tempBuffer.delete(0, tempBuffer.length());
				}
				// 不是开始结束符，也不是分隔符的情况
				else {
					tempBuffer.append(currentChar);
				}
			}
		}
		/*
		 * 处理剩余的字符。 例如：1,2,3,4,5 最后这个5，无法在for循环中被加入结果，所以，在循环结束后，需要处理一下。
		 */
		if (tempBuffer.length() > 0)
			resultList.add(tempBuffer.toString());

		return handleResultList(resultList);
	}

	/*
	 * 将用户指定的开始结束符（这种形式：{}|“”|()），解析并放用map中
	 */
	private void resolveWrapSigns() {
		String[] splitedPairs = wrapSigns.split("|");
		for (String pair : splitedPairs) {
			char[] chs = pair.toCharArray();
			if (chs.length != 2) {
				throw new IllegalArgumentException("指定的开始结束符不正确");
			}
			signs.put(chs[0], chs[1]);
		}
	}

	/* 判断一个字符是否是开始结束符号 */
	private boolean isSign(char ch, boolean isStart) {
		// 此迭代器中存放的是当前使用的开始结束符号，便于isSign方法使用 <开始符，结束符>
		Iterator<Entry<Character, Character>> signsIterator = signs.entrySet().iterator();

		while (signsIterator.hasNext()) {
			Entry<Character, Character> current = signsIterator.next();
			if (isStart) {
				if (ch == current.getKey()) {
					return true;
				}
			} else {
				if (ch == current.getValue()) {
					return true;
				}
			}
		}

		return false;
	}

	/* 判断一个字符是否为开始符 */
	private boolean isStart(char ch) {
		return isSign(ch, true);
	}

	/* 判断一个字符是否为结束符 */
	private boolean isEnd(char ch) {
		return isSign(ch, false);
	}

	/*
	 * 检查一个开始结束符，判据这个字符的开始与结束是不是一样的，例如： ' '
	 */
	private boolean isSameStartEnd(char ch) {
		Character value = signs.get(ch);
		return value != null && value == ch;
	}

	/* 在一个char数组中截取出字符串 */
	private String subCharArray(char[] charArray, int start, int end) {
		StringBuilder buffer = new StringBuilder();
		for (int i = start; i <= end; i++) {
			buffer.append(charArray[i]);
		}
		if (!keepSign) {
			trimSign(buffer);
		}
		return buffer.toString();
	}

	/*
	 * 用于去掉字段两边的开始结束符
	 */
	private void trimSign(StringBuilder buffer) {
		if (isStart(buffer.charAt(0))) {
			buffer.delete(0, 1);
		}
		if (isEnd(buffer.charAt(buffer.length() - 1))) {
			buffer.delete(buffer.length() - 1, buffer.length());
		}
	}

	/*
	 * 处理结果List
	 */
	private String[] handleResultList(List<String> resultList) {
		List<String> handledList = new ArrayList<String>();

		int start = 0;
		int end = 0;
		boolean started = false;
		for (int i = 0; i < resultList.size(); i++) {
			String field = resultList.get(i);
			if (isSameStartEnd(field.length() == 0 ? '\0' : field.charAt(0)) && !started) {
				start = i;
				started = true;
			}
			if (isSameStartEnd(field.length() == 0 ? '\0' : field.charAt(field.length() - 1)) && started) {
				end = i;
			}
		}

		if (start == 0) {
			if (resultList.size() > 0) {
				resultList.remove(0);
			}
			return resultList.toArray(new String[0]);
		}

		StringBuilder buffer = new StringBuilder();
		for (int i = start; i <= end; i++) {
			buffer.append(resultList.get(i));
			if (i != end) {
				buffer.append(value);
			}
		}
		if (!keepSign) {
			trimSign(buffer);
		}

		boolean inseted = false;
		for (int i = 0; i < resultList.size(); i++) {
			if (i < start || i > end) {
				handledList.add(resultList.get(i));
			} else if (!inseted) {
				handledList.add(buffer.toString());
				inseted = true;
			}
		}
		if (handledList.size() > 0) {
			handledList.remove(0);
		}
		return handledList.toArray(new String[0]);
	}

	public static void main(String[] args) {
		String data = "sfds,,\"sdfs,<sdf,ok>,sdfsdfd,<2<33,3324>,sdf>,rwrwe";
		LogicSpliter lo = new LogicSpliter(',');
		lo.setKeepSign(true);
		String[] ss = (String[]) lo.apply(data);
		for (String s : ss) {
			System.out.println(s);
		}
	}
}
