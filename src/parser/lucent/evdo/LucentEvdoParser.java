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
public class LucentEvdoParser implements EvdoParser {

	// 指向话单输入流
	private InputStream in;

	// 话单解释回调接口
	private EvdoParserCallback callback;

	// 话单日期
	private String stampTime;

	// 空格
	private static final String WHITE_SPACE = " ";

	// RNC段的开始标记
	public static final String RNC = "RNC";

	// TP段的开始标记
	public static final String TP = "TP";

	// OHM段的开始标记
	public static final String OHM = "OHM";

	// HCS段的开始标记
	public static final String HCS = "HCS";

	// SECT段的开始标记
	public static final String SECT = "SECT";

	// CARR段的开始标记
	public static final String CARR = "CARR";

	// AP段的开始标记
	public static final String AP = "AP";

	// 列名与值的分隔符
	private static final String SPLITE = ",";

	// 文件结束标记
	private static final String END_FLAG = "LEAD" + WHITE_SPACE + AP;

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	/**
	 * 构造方法，未指定话单文件的输入流。
	 * 
	 * @see #setInputStream(InputStream)
	 * @see #LucentEvdoParser(InputStream)
	 */
	public LucentEvdoParser() {
		super();
	}

	/**
	 * 构造方法，指定话单文件的输入流。
	 * 
	 * @param in
	 */
	public LucentEvdoParser(InputStream in) {
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
		String prefix = ""; // 字段名的前缀
		List<EvdoRecordPair> records = new ArrayList<EvdoRecordPair>(); // 存放一个宿主的键值对

		String lastHcsId = null;
		String lastRncId = null;
		String lastSectId = null;

		try {
			line = reader.readLine(); // 读一行
			while (line != null && !line.startsWith(END_FLAG)) {
				if (isOnwerStart(line)) // 遇到了RNC,TP这样的开始标记
				{
					if (records.size() > 0 && owner != null) {
						if (owner.equals(HCS) && line.startsWith(CARR + WHITE_SPACE)) {
							EvdoRecordPair carrpair = new EvdoRecordPair(CARR + "_ID", line.split(WHITE_SPACE)[1].trim());
							if (!records.contains(carrpair)) {
								records.add(carrpair);
							}
							line = reader.readLine();
							while (!line.startsWith(SECT) && !line.startsWith(CARR)) {
								String[] spliteds = line.split(SPLITE);
								if (spliteds.length == 2) {
									EvdoRecordPair pair = new EvdoRecordPair(spliteds[0], spliteds[1]);
									if (!records.contains(pair)) {
										records.add(pair);
									}
								} else {
									EvdoRecordPair pair = new EvdoRecordPair(spliteds[0], String.valueOf(0));
									if (!records.contains(pair)) {
										records.add(pair);
									}
								}
								line = reader.readLine();
							}
							continue;
						} else {
							callback.handleData(records, owner, id, omcId, stampTime, taskID);
							records.clear();
							id = null;
							prefix = "";
						}
					}
					if (isOnwerStart(line)) {
						// lastHcsId = null;
						// lastRncId = null;
						// lastSectId = null;
						id = line.split(WHITE_SPACE)[1].trim();
						if (line.startsWith(RNC)) {
							owner = RNC;
							lastRncId = id;
						} else if (line.startsWith(OHM)) {
							owner = OHM;
						} else if (line.startsWith(AP)) {
							owner = AP;
						} else if (line.startsWith(TP)) {
							owner = TP;
						} else if (line.startsWith(HCS)) {
							owner = HCS;
							lastHcsId = line.split(WHITE_SPACE)[1].trim();
						} else if (line.startsWith(SECT)) {
							owner = SECT;
							lastSectId = id;
						} else if (line.startsWith(CARR)) {
							owner = CARR;
						}
					}

				} else if (owner != null) // 没遇到开始标记，但是确定当宿主是谁
				{
					String[] splitedItems = line.split(SPLITE);
					if (splitedItems.length != 2) {
						splitedItems = line.split(":");
					}
					if (splitedItems.length == 2) {
						EvdoRecordPair idPair = new EvdoRecordPair(owner + "_ID", id);
						String pairName = prefix.length() == 0 ? splitedItems[0] : prefix + "_" + splitedItems[0];
						EvdoRecordPair pair = new EvdoRecordPair(pairName, splitedItems[1].trim());

						if (records.size() == 0) {
							records.add(idPair);
						}
						if (owner.equals(OHM) || owner.equals(TP) || owner.equals(AP) || owner.equals(CARR)) {
							String fkName = null;
							String fkValue = null;
							EvdoRecordPair fkPair = null;

							if (owner.equals(OHM) || owner.equals(TP) || owner.equals(AP)) {
								fkName = RNC + "_ID";
								fkValue = lastRncId;
							} else if (owner.equals(SECT)) {
								fkName = HCS + "_ID";
								fkValue = lastHcsId;
							} else if (owner.equals(CARR)) {
								fkName = SECT + "_ID";
								fkValue = lastSectId;
							}

							if (lastHcsId != null) {
								EvdoRecordPair hcspair = new EvdoRecordPair(HCS + "_ID", lastHcsId);

								if (!records.contains(hcspair)) {
									records.add(hcspair);
								}
							}

							fkPair = new EvdoRecordPair(fkName, fkValue);
							if (!records.contains(fkPair)) {
								records.add(fkPair);
							}
						}
						if (!records.contains(pair)) {
							records.add(pair);
						}
					} else {
						prefix = line.replace(" ", "_");
					}
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
				prefix = "";
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

	private String fileNameToDate(String fileName) {
		char[] cs = fileName.toCharArray();
		char[] array = new char[12];
		int index = 0;
		for (char c : cs) {
			if (index == 12) {
				break;
			}
			if (Character.isDigit(c)) {
				array[index++] = c;
			}
		}

		String dstr = new String(array);

		return dstr.substring(0, 4) + "-" + dstr.substring(4, 6) + "-" + dstr.substring(6, 8) + " " + dstr.substring(8, 10) + ":"
				+ dstr.substring(10, 12) + ":00";
	}

	public static void main(String[] args) {
		LucentEvdoParser p = new LucentEvdoParser();
		System.out.println(p.fileNameToDate("AG_201001211400GMT.HDRFMS020"));
	}

	/* 判断一行是不是一个宿主的开始 */
	private boolean isOnwerStart(String line) {
		return line.startsWith(RNC + WHITE_SPACE) || line.startsWith(TP + WHITE_SPACE) || line.startsWith(AP + WHITE_SPACE)
				|| line.startsWith(OHM + WHITE_SPACE) || line.startsWith(HCS + WHITE_SPACE) || line.startsWith(CARR + WHITE_SPACE)
				|| line.startsWith(SECT + WHITE_SPACE);
	}
}
