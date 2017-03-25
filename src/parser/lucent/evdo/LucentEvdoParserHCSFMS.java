package parser.lucent.evdo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import util.LogMgr;
import util.Util;

/**
 * 朗讯EVDO话单解析实现。
 * 
 * @author ChenSijiang 2009.01.25
 * @since 1.0
 */
public class LucentEvdoParserHCSFMS implements EvdoParser {

	// 指向话单输入流
	private InputStream in;

	// 话单解释回调接口
	private EvdoParserCallback callback;

	// 话单日期
	private String stampTime;

	// 空格
	private static final String WHITE_SPACE = " ";

	// SECT段的开始标记
	public static final String SECT = "SECT";

	// CARR段的开始标记
	public static final String CARR = "CARR";

	// HCS段的开始标记
	public static final String HCS = "HCS";

	// FACTYPE段的开始标记
	public static final String FACTYPE = "FACTYPE";

	// 列名与值的分隔符
	private static final String SPLITE = ",";

	// 文件结束标记
	private static final String END_FLAG = "EndFile";

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	/**
	 * 构造方法，未指定话单文件的输入流。
	 * 
	 * @see #setInputStream(InputStream)
	 * @see #LucentEvdoParser(InputStream)
	 */
	public LucentEvdoParserHCSFMS() {
		super();
	}

	/**
	 * 构造方法，指定话单文件的输入流。
	 * 
	 * @param in
	 */
	public LucentEvdoParserHCSFMS(InputStream in) {
		this();
		this.in = in;
	}

	@Override
	public void parse(EvdoParserCallback callback, String omcId, Timestamp time, boolean autoDispose, long taskID) throws EvdoParseException {
		this.callback = callback;
		stampTime = Util.getDateString(time);
		check();
		// 根据话单输入流产生BufferedReader，便于逐行扫描
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line = null; // 读出的一行
		String owner = null; // 当前数据宿主名
		String id = null; // 当前数据的标识
		List<EvdoRecordPair> records = new ArrayList<EvdoRecordPair>(); // 存放一个宿主的键值对

		String lastSectId = null;
		String lastHcsId = null;

		try {
			line = reader.readLine(); // 读一行
			while (line != null && !line.startsWith(END_FLAG)) {
				// if ( records.size() == 231 || records.size() == 2 )
				if (records.size() > 0) {
					callback.handleData(records, owner, id, omcId, stampTime, taskID);
					records.clear();
				} else {
					records.clear();
				}

				if (isOnwerStart(line)) // 遇到了RNC,TP这样的开始标记
				{
					if (line.startsWith(HCS)) {
						owner = HCS;
						String[] splited = line.split(WHITE_SPACE);
						lastHcsId = splited[1].trim();
						id = splited[1].trim();
						line = reader.readLine();
						while (line != null && !line.startsWith(FACTYPE) && !line.startsWith("NO ")) {
							splited = line.split(WHITE_SPACE);
							String name = splited[0].trim();
							String value = splited[1].trim();
							if (!name.startsWith(END_FLAG)) {
								records.add(new EvdoRecordPair(name, value));
							}
							line = reader.readLine();
							if (line == null) {
								break;
							}
						}
						continue;
					} else if (line.startsWith(SECT)) {
						String[] splited = line.split(WHITE_SPACE);
						lastSectId = splited[1].trim();
					} else if (line.startsWith(CARR)) {
						owner = CARR;
						String[] splited = line.split(WHITE_SPACE);
						id = splited[1].trim();
						line = reader.readLine();
						while (line != null && !line.startsWith(SECT) && !line.startsWith(HCS)) {
							splited = line.split(SPLITE);
							EvdoRecordPair idPair = new EvdoRecordPair(CARR + "_ID", id);
							EvdoRecordPair hcsPair = new EvdoRecordPair(HCS + "_ID", lastHcsId);
							EvdoRecordPair sectPair = new EvdoRecordPair(SECT + "_ID", lastSectId);
							if (!records.contains(idPair)) {
								records.add(idPair);
							}
							if (!records.contains(hcsPair)) {
								records.add(hcsPair);
							}
							if (!records.contains(sectPair)) {
								records.add(sectPair);
							}
							// add 20100917
							if (splited != null && splited.length < 2) {
								line = reader.readLine();
								if (line == null) {
									break;
								}
								if (line.startsWith("NO ")) {
									line = reader.readLine();
								}
								continue;
							}
							// end
							records.add(new EvdoRecordPair(splited[0].trim(), splited[1].trim()));
							line = reader.readLine();
							if (line == null) {
								break;
							}
							if (line.startsWith("NO ")) {
								line = reader.readLine();
							}
						}
						continue;
					}
				} else if (owner != null) // 没遇到开始标记，但是确定当宿主是谁
				{

				} else
				// 没遇到开始标记，也没有宿主，则忽略
				{

				}
				line = reader.readLine(); // 读一行
			}
			if (records.size() > 0 && owner != null) {
				callback.handleData(records, owner, id, omcId, stampTime, taskID);
				records.clear();
				id = null;
			}
			callback.handleData(null, owner, null, null, null, taskID); // 传入null，通知回调者，已经结束
		} catch (Exception e) {
			throw new EvdoParseException("解析话单时异常", e);
		} finally {
			if (autoDispose) {
				dispose();
			}
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
				}
			}
		}
	}

	@Override
	public InputStream getInputStream() {
		return in;
	}

	@Override
	public void setInputStream(InputStream in) {
		this.in = in;
	}

	@Override
	public void dispose() {
		if (in != null) {
			try {
				in.close();
			} catch (IOException e) {
				logger.error("关闭话单输入流时，产生了一个异常：" + e.getMessage());
			}
		}
	}

	/* 检查传入的参数 */
	private void check() throws EvdoParseException {
		if (callback == null) {
			throw new EvdoParseException("回调接口为null");
		}
		if (in == null) {
			throw new EvdoParseException("话单文件输入流为null");
		}
	}

	// private String fileNameToDate(String fileName)
	// {
	// char[] cs = fileName.toCharArray();
	// char[] array = new char[12];
	// int index = 0;
	// for (char c : cs)
	// {
	// if ( index == 12 )
	// {
	// break;
	// }
	// if ( Character.isDigit(c) )
	// {
	// array[index++] = c;
	// }
	// }
	//
	// String dstr = new String(array);
	//
	// return dstr.substring(0, 4) + "-" + dstr.substring(4, 6) + "-"
	// + dstr.substring(6, 8) + " " + dstr.substring(8, 10) + ":"
	// + dstr.substring(10, 12) + ":00";
	// }

	/* 判断一行是不是一个宿主的开始 */
	private boolean isOnwerStart(String line) {
		return line.startsWith(CARR + WHITE_SPACE) || line.startsWith(SECT + WHITE_SPACE) || line.startsWith(HCS + WHITE_SPACE);
	}
}
