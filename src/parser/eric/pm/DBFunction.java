package parser.eric.pm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;

/**
 * 通过Java语言实现数据库汇总中的一些函数。
 * 
 * @author ChenSijiang 2011-2-22 下午03:11:00
 */
public strictfp final class DBFunction {

	public static final Map<Integer, CalVals> CFG = new HashMap<Integer, CalVals>();

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final List<CalCfgItem> ITEMS = new ArrayList<CalCfgItem>();

	static {
		loadV();
		loadC();
	}

	private static void loadV() {
		// 载入clt_pm_w_eric_cal_vals表

		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		String sql = "select * from clt_pm_w_eric_cal_vals order by counter_group,counter_index";
		try {
			con = DbPool.getConn();
			st = con.prepareStatement(sql);
			rs = st.executeQuery();
			while (rs.next()) {
				int group = rs.getInt("counter_group");
				int index = rs.getInt("counter_index");
				String strR = rs.getString("raw_val");
				String strU = rs.getString("uway_val");
				Double rval = Util.isNull(strR) ? null : Double.parseDouble(strR);
				Double uval = Util.isNull(strU) ? null : Double.parseDouble(strU);

				String strLeft = rs.getString("left_val");
				String strMid = rs.getString("mid_val");
				String strRight = rs.getString("right_val");
				Double left = Util.isNull(strLeft) ? null : Double.parseDouble(strLeft);
				Double mid = Util.isNull(strMid) ? null : Double.parseDouble(strMid);
				Double right = Util.isNull(strRight) ? null : Double.parseDouble(strRight);

				CalVals calCfg = null;
				if (CFG.containsKey(group))
					calCfg = CFG.get(group);
				else {
					calCfg = new CalVals(group);
					CFG.put(group, calCfg);
				}
				calCfg.indexValues.put(index, new Val(rval, uval, left, mid, right));
			}
		} catch (Exception e) {
			logger.error("载入clt_pm_w_eric_cal_vals表时异常，sql - " + sql, e);
		} finally {
			CommonDB.close(rs, st, con);
		}
	}

	private static void loadC() {
		// 载入clt_pm_w_eric_cal_cfg表

		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		String sql = "select  * from clt_pm_w_eric_cal_cfg ";
		try {
			con = DbPool.getConn();
			st = con.createStatement();
			rs = st.executeQuery(sql);
			while (rs.next()) {
				String strMax = rs.getString("max_direction");
				String strAvg = rs.getString("avg_direction");
				int calType = rs.getInt("CAL_TYPE");

				int max = DEFAULT_VAL;
				int avg = DEFAULT_VAL;

				if (Util.isNotNull(strMax)) {
					strMax = strMax.trim().toLowerCase();
					if (strMax.equals("left"))
						max = LEFT_VAL;
					else if (strMax.equals("mid"))
						max = MID_VAL;
					else if (strMax.equals("right"))
						max = RIGHT_VAL;

				}

				if (Util.isNotNull(strAvg)) {
					strAvg = strAvg.trim().toLowerCase();
					if (strAvg.equals("left"))
						avg = LEFT_VAL;
					else if (strAvg.equals("mid"))
						avg = MID_VAL;
					else if (strAvg.equals("right"))
						avg = RIGHT_VAL;

				}

				ITEMS.add(new CalCfgItem(rs.getString("counter_name"), rs.getString("col_name"), rs.getString("tab_name"),
						rs.getInt("counter_group"), rs.getString("addition_col_max"), rs.getString("addition_col_avg"), max, avg, calType));
			}
		} catch (Exception e) {
			logger.error("载入clt_pm_w_eric_cal_cfg表时异常，sql - " + sql, e);
		} finally {
			CommonDB.close(rs, st, con);
		}
	}

	/**
	 * <pre>
	 *  参数：
	 *   cal_type 取值：1 MAX ; 2 AVG;
	 *   group_type 取值：1  2  3  4 
	 *   COUNTER名称: PMTRANSMITTEDCARRIERPOWER,  PMTRANSMITTEDCARRIERPOWERNONHS,  PMTRANSMITTEDCARRIERPOWERHS
	 *   第一组，即 group_type 为 1
	 *   最大值：从右至左，找到第一个采样点不为 0 的，返回该点的 数据库转换（固定）；
	 *   平均值：把所有的采样点功率相加,再除以所有的采样点总数,就是其平均功率;
	 *   提示：1、如果采样点全都是0，则最大值返回第一个 数据库转换（固定)的值；平均值与最大值取法一致；
	 *         2、如果采样点全都是null，即,,,,,,,,,,，则最大值返回null;        平均值与最大值取法一致；
	 *         3、如果采样点有0，有null，即0,,,,0,,,，则最大值返回第一个 数据库转换（固定）的值；平均值亦是；同1
	 *         4、如果采样点有非零值，即1,2,3,,,4,或0,0,0,3,4,0,,,则，按正规取法取值（最大值、平均值）
	 *         5、如果采样点按正常逻辑，即0,0,0,0,5,6,0,0 ，则按正规取法取值（最大值、平均值）
	 *   第二组，即 group_type 为 2
	 *   最大值：从右至左，找到第一个采样点不为 0 的，返回该点的 原始值（固定）；
	 *   平均值：把所有的采样点功率相加,再除以所有的采样点总数,就是其平均功率;
	 *   提示：1、如果采样点全都是0，则最大值返回第一个 数据库转换（固定)的值；平均值与最大值取法一致；
	 *         2、如果采样点全都是null，即,,,,,,,,,,，则最大值返回null;        平均值与最大值取法一致；
	 *         3、如果采样点有0，有null，即0,,,,0,,,，则最大值返回第一个 数据库转换（固定）的值；平均值亦是；同1
	 *         4、如果采样点有非零值，即1,2,3,,,4,或0,0,0,3,4,0,,,则，按正规取法取值（最大值、平均值）
	 *         5、如果采样点按正常逻辑，即0,0,0,0,5,6,0,0 ，则按正规取法取值（最大值、平均值）
	 *   该提示不只用于第二组，同样适用于第三组。
	 *   第三组，即 group_type 为 3
	 *   见第二组说明
	 *   第四组，即 group_type 为 4
	 *   最大值：从右至左，找到第一个采样点不为 0 的，返回该点的 原始值（固定）；
	 *   平均值：把所有的采样点功率相加,再除以所有的采样点总数,就是其平均功率，在经过公式 X=10*LOG(Y*1000,10) 转换，
	 *           得到的结果就是平均值 X；
	 *   提示：1、如果采样点全都是0，则最大值返回第一个 数据库转换（固定)的值；平均值与最大值取法一致；
	 *         2、如果采样点全都是null，即,,,,,,,,,,，则最大值返回null;        平均值与最大值取法一致；
	 *         3、如果采样点有0，有null，即0,,,,0,,,，则最大值返回第一个 数据库转换（固定）的值；平均值亦是；同1
	 *         4、如果采样点有非零值，即1,2,3,,,4,或0,0,0,3,4,0,,,则，按正规取法取值（最大值、平均值）
	 *         5、如果采样点按正常逻辑，即0,0,0,0,5,6,0,0 ，则按正规取法取值（最大值、平均值）
	 *   注解：
	 *   SOURCE_FIXED        原始值（固定）
	 *   TRANSFER_FIXED      数据库转换（固定）
	 *   UWAY_SAMPLE         UWAY采集表值（采样点数）
	 *   COMPUTE_RESULT      计算结果
	 * </pre>
	 * 
	 * @param counterList
	 *            原始counter值（以逗号分隔的）。
	 * @param calType
	 *            1=求MAX；2=求AVG;3=求MIN; 4 SUM
	 * @param groupType
	 *            取值范围：1,2,3,4.
	 * @return 计算结果。
	 * @throws IllegalArgumentException
	 *             传入参数不正确。
	 */
	public static Double getMaxAvgCounter(String counterList, int calType, int groupType, CalCfgItem calcfgItem) {
		if (calType != 1 && calType != 2 && calType != 3)
			throw new IllegalArgumentException("calType");
		if (groupType < 1 || groupType > 6)
			throw new IllegalArgumentException("groupType");

		String src = counterList;
		if (Util.isNull(src))
			return null;
		Double[] counters = parseCounterList(counterList);

		int state = -1;
		CalVals cfg = null;
		switch (groupType) {
		// group 1
			case 1 :
				cfg = CFG.get(1);
				state = getCountersState(counters);
				// max
				if (calType == 1) {
					if (state == ALL_ZERO)
						return toDBConvert(convertDirection(cfg, 1, calcfgItem.maxDirection));
					if (state == ALL_NULL)
						return null;
					if (state == ZERO_NULL)
						return toDBConvert(convertDirection(cfg, 1, calcfgItem.maxDirection));
					for (int i = counters.length - 1; i > -1; i--) {
						if (counters[i] != null && counters[i].intValue() != 0) {
							return toDBConvert(convertDirection(cfg, i + 1, calcfgItem.maxDirection));
						}
					}
				}
				// avg
				else if (calType == 2) {

					if (state == ALL_ZERO)
						return toDBConvert(convertDirection(cfg, 1, calcfgItem.avgDirection));
					if (state == ALL_NULL)
						return null;
					if (state == ZERO_NULL)
						return toDBConvert(convertDirection(cfg, 1, calcfgItem.avgDirection));
					double d1 = 0.0;// 所有的采样点功率之和
					double d2 = 0.0;// 所有的采样点总数
					for (int i = 0; i < counters.length; i++) {
						d1 += (toDBConvert(convertDirection(cfg, i + 1, calcfgItem.avgDirection)) * counters[i]);
						d2 += (counters[i]);
					}
					return d1 / d2;

				}

				break;
			// group 2
			case 2 :
				cfg = CFG.get(2);
				state = getCountersState(counters);
				// max
				if (calType == 1) {
					if (state == ALL_ZERO)
						return toDBConvert(cfg.indexValues.get(1).rawVal);
					if (state == ALL_NULL)
						return null;
					if (state == ZERO_NULL)
						return toDBConvert(cfg.indexValues.get(1).rawVal);
					for (int i = counters.length - 1; i > -1; i--) {
						if (counters[i] != null && counters[i].intValue() != 0) {
							return cfg.indexValues.get(i + 1).rawVal;
						}
					}
				}
				// avg
				else if (calType == 2) {
					if (state == ALL_ZERO)
						return toDBConvert(cfg.indexValues.get(1).rawVal);
					if (state == ALL_NULL)
						return null;
					if (state == ZERO_NULL)
						return toDBConvert(cfg.indexValues.get(1).rawVal);
					double d1 = 0.0;// 所有的采样点功率之和
					double d2 = 0.0;// 所有的采样点总数
					for (int i = 0; i < counters.length; i++) {
						d1 += (cfg.indexValues.get(i + 1).rawVal * counters[i]);
						d2 += (counters[i]);
					}
					return d1 / d2;

				}

				break;
			// group 3
			case 3 :
				cfg = CFG.get(3);
				state = getCountersState(counters);
				// max
				if (calType == 1) {
					if (state == ALL_ZERO)
						return toDBConvert(convertDirection(cfg, 1, calcfgItem.maxDirection));
					if (state == ALL_NULL)
						return null;
					if (state == ZERO_NULL)
						return toDBConvert(convertDirection(cfg, 1, calcfgItem.maxDirection));
					for (int i = counters.length - 1; i > -1; i--) {
						if (counters[i] != null && counters[i].intValue() != 0) {
							return convertDirection(cfg, i + 1, calcfgItem.maxDirection);
						}
					}
				}
				// avg 第一个采样点不参与计算。
				else if (calType == 2) {
					if (state == ALL_ZERO)
						return toDBConvert(convertDirection(cfg, 2, calcfgItem.avgDirection));
					if (state == ALL_NULL)
						return null;
					if (state == ZERO_NULL)
						return toDBConvert(convertDirection(cfg, 2, calcfgItem.avgDirection));
					double d1 = 0.0;// 所有的采样点功率之和
					double d2 = 0.0;// 所有的采样点总数
					for (int i = 1; i < counters.length; i++) {
						d1 += (convertDirection(cfg, i + 1, calcfgItem.avgDirection) * counters[i]);
						d2 += (counters[i]);
					}
					return d1 / d2;

				}

				break;
			// group 4
			case 4 :
				cfg = CFG.get(4);
				state = getCountersState(counters);
				// max
				if (calType == 1) {
					if (state == ALL_ZERO)
						return toDBConvert(convertDirection(cfg, 1, calcfgItem.maxDirection));
					if (state == ALL_NULL)
						return null;
					if (state == ZERO_NULL)
						return toDBConvert(convertDirection(cfg, 1, calcfgItem.maxDirection));
					for (int i = counters.length - 1; i > -1; i--) {
						if (counters[i] != null && counters[i].intValue() != 0)
							return convertDirection(cfg, i + 1, calcfgItem.maxDirection);
					}
				}
				// avg
				else if (calType == 2) {
					if (state == ALL_ZERO)
						return toDBConvert(convertDirection(cfg, 1, calcfgItem.avgDirection));
					if (state == ALL_NULL)
						return null;
					if (state == ZERO_NULL)
						return toDBConvert(convertDirection(cfg, 1, calcfgItem.avgDirection));
					double d1 = 0.0;// 所有的采样点功率之和
					double d2 = 0.0;// 所有的采样点总数
					for (int i = 0; i < counters.length; i++) {
						d1 += ((toDBConvert(convertDirection(cfg, i + 1, calcfgItem.avgDirection))) * counters[i]);
						d2 += (counters[i]);
					} // X=10*LOG(Y*1000,10)
					return 10 * StrictMath.log10(d1 / d2 * 1000);

					// min 从左到右第一个不为0的I
				} else if (calType == 3) {
					if (state == ALL_ZERO)
						return toDBConvert(convertDirection(cfg, 1, calcfgItem.maxDirection));
					if (state == ALL_NULL)
						return null;
					if (state == ZERO_NULL)
						return toDBConvert(convertDirection(cfg, 1, calcfgItem.maxDirection));
					for (int i = 0; i < counters.length; i++) {
						if (counters[i] != null && counters[i].intValue() != 0)
							return convertDirection(cfg, i + 1, calcfgItem.maxDirection);
					}
				}
				break;
			// group 5 这里是2011.4.25新需求，即将逗号分隔的所有数字相加(有两个计数器。 pmCapacityNodeBDlCe
			// 下行平均CE占用率% pmCapacityNodeBUlCe 上行平均CE占用率%)
			case 5 :
				double val = 0.00;
				int c = 0;
				for (int i = 1; i < counters.length; i++) {
					Double d = counters[i];
					if (d != null) {
						val += d;
						c++;
					}
				}
				if (c == 0)
					return val;
				else
					return val / c;

				/**
				 * <pre>
				 * group 6, 是2011.6.7的新需要。CLT_PM_W_ERIC_HSDSCHRESOURCES的pmUsedHsPdschCodes字段，算法为：
				 * 				这个字段一共16个值，是数组，计算公式如下：
				 * 				[0] : 0 codes
				 * 				[1] : 1 codes
				 * 				..
				 * 				[15] : 15 codes
				 * 				
				 * 				
				 * 				公式 = ([0]*0 + [1]*1 + [2]*2 + .....[15] * 15)/([0] +[1]+[2] + .....[15])
				 * </pre>
				 */
			case 6 :
				cfg = CFG.get(6);
				if (calType == 1) {
					double part1 = 0.00;
					int part2 = 0;
					for (int i = 0; i < counters.length; i++) {
						// liangww add 2012-04-01 增加判断为空状态
						Double counter = counters[i];
						if (counter == null)
							counter = Double.valueOf(0);

						part1 += (counter * i);
						part2 += counter;
					}
					if (part2 == 0)// 避免除数为0
						part2 = 1;
					return part1 / part2;
				} else if (calType == 2) {
					// calType为2时，计算HSDPA
					// 码资源最大占用个数，计算结果放入USED_HS_PDSCH_CODES_CAL_2列，20120329需求。
					state = getCountersState(counters);
					if (state == ALL_ZERO)
						return toDBConvert(convertDirection(cfg, 1, calcfgItem.maxDirection));
					if (state == ALL_NULL)
						return null;
					if (state == ZERO_NULL)
						return toDBConvert(convertDirection(cfg, 1, calcfgItem.maxDirection));
					for (int i = counters.length - 1; i > -1; i--) {
						if (counters[i] != null && counters[i].intValue() != 0) {
							return convertDirection(cfg, i + 1, calcfgItem.maxDirection);
						}
					}
				}
			default :
				break;
		}

		return null;
	}

	/**
	 * @param counterList
	 * @param calType
	 *            4:getSum 5:getMaxI 6:getMinI 7:getIncreaseSum 8:getMappingSum
	 * @param groupType
	 * @param calcfgItem
	 * @return
	 */
	public static Double getValueFromCounter(String counterList, int groupType, CalCfgItem calcfgItem) {
		Double[] counters = parseCounterList(counterList);
		switch (calcfgItem.calType) {
			case 4 :
				return getSum(counters);
			case 5 :
				return getMaxIMapping(counters, groupType, calcfgItem);
			case 6 :
				return getMinIMapping(counters, groupType, calcfgItem);
			case 7 :
				return getIncreaseSum(counters);
			case 8 :
				return getMappingSum(counters, groupType, calcfgItem);
			default :
				break;
		}
		return null;
	}

	/**
	 * Array[0]+ Array[1] + Array[2] + ......Array[Array.length]
	 */
	private static Double getSum(Double[] counters) {
		if (counters == null || counters.length == 0) {
			return null;
		}
		double sum = 0;
		for (int i = 0; i < counters.length; i++) {
			if (counters[i] != null) {
				sum += counters[i];
			}
		}
		return sum;
	}

	/**
	 * 从counters的最大下标开始往回找到第一个不为0的值的下标，并且返回该下标在配置中对应的值
	 */
	private static Double getMaxIMapping(Double[] counters, int groupId, CalCfgItem calcfgItem) {
		int state = getCountersState(counters);
		CalVals cfg = CFG.get(groupId);
		// max
		if (state == ALL_ZERO)
			return toDBConvert(convertDirection(cfg, 1, calcfgItem.maxDirection));
		if (state == ALL_NULL)
			return null;
		if (state == ZERO_NULL)
			return toDBConvert(convertDirection(cfg, 1, calcfgItem.maxDirection));
		for (int i = counters.length - 1; i > -1; i--) {
			if (counters[i] != null && counters[i].intValue() != 0) {
				return toDBConvert(convertDirection(cfg, i, calcfgItem.maxDirection));
			}
		}
		return null;
	}

	/**
	 * 从counters的最小下标开始找到第一个不为0的值的下标，并且返回该下标在配置中对应的值
	 */
	private static Double getMinIMapping(Double[] counters, int groupId, CalCfgItem calcfgItem) {
		int state = getCountersState(counters);
		CalVals cfg = CFG.get(groupId);
		// max
		if (state == ALL_ZERO)
			return toDBConvert(convertDirection(cfg, 1, calcfgItem.maxDirection));
		if (state == ALL_NULL)
			return null;
		if (state == ZERO_NULL)
			return toDBConvert(convertDirection(cfg, 1, calcfgItem.maxDirection));
		for (int i = 0; i < counters.length; i++) {
			if (counters[i] != null && counters[i].intValue() != 0) {
				return toDBConvert(convertDirection(cfg, i, calcfgItem.maxDirection));
			}
		}
		return null;
	}

	/**
	 * 0*Array[0]+ 1*Array[1] + 2*Array[2] + ......n*Array[Array.length]
	 */
	private static Double getIncreaseSum(Double[] counters) {
		if (counters == null || counters.length == 0) {
			return null;
		}
		double sum = 0;
		for (int i = 0; i < counters.length; i++) {
			sum += i * counters[i];
		}
		return sum;
	}

	/**
	 * Mapping[0]*Array[0]+ Mapping[1]*Array[1] + Mapping[2]*Array[2] + ...... Mapping[Array.length]*Array[Array.length]
	 * 
	 * @return
	 */
	private static Double getMappingSum(Double[] counters, int groupId, CalCfgItem calcfgItem) {
		int state = getCountersState(counters);
		CalVals cfg = CFG.get(groupId);
		if (state == ALL_ZERO)
			return toDBConvert(convertDirection(cfg, 2, calcfgItem.maxDirection));
		if (state == ALL_NULL)
			return null;
		if (state == ZERO_NULL)
			return toDBConvert(convertDirection(cfg, 2, calcfgItem.maxDirection));
		double sum = 0.0;// 所有的采样点功率之和
		for (int i = 1; i < counters.length; i++) {
			if (counters[i] != null) {
				sum += (convertDirection(cfg, i, calcfgItem.maxDirection) * counters[i]);
			}
		}
		return sum;
	}

	static final int NORMAL = 0;// 正常

	static final int ALL_ZERO = 1; // 全都是0

	static final int ALL_NULL = 2; // 全都是null

	static final int ZERO_NULL = 3; // 有0，有null

	static final int NULL_NORMAL = 4; // 有非零值，有null

	private static int getCountersState(Double[] counters) {
		if (counters == null)
			return ALL_NULL;
		boolean findNull = false;
		boolean findZero = false;
		boolean findNormal = false;
		for (Double d : counters) {
			if (d == null)
				findNull = true;
			else if (d.intValue() == 0)
				findZero = true;
			else
				findNormal = true;
		}

		if (findZero && !findNormal && !findNull)
			return ALL_ZERO;
		if (findNull && !findNormal && !findZero)
			return ALL_NULL;
		if (findZero && findNull && !findNormal)
			return ZERO_NULL;
		if (findNormal && findNull)
			return NULL_NORMAL;

		return NORMAL;
	}

	private static Double[] parseCounterList(String counterList) {
		if (Util.isNull(counterList))
			return null;

		String[] sp = counterList.split(",", 9999);
		Double[] result = new Double[sp.length];
		for (int i = 0; i < sp.length; i++) {
			if (Util.isNull(sp[i]))
				result[i] = null;
			else
				result[i] = Double.parseDouble(sp[i]);
		}

		return result;
	}

	private DBFunction() {
		super();
	}

	private static Double convertDirection(CalVals cfg, int index, int direction) {
		// add for tyler.lee 20151110 begin
		if (cfg.indexValues.get(index) == null) {
			return null;
		}
		// add for tyler.lee 20151110 end
		Double raw = cfg.indexValues.get(index).rawVal;

		Double val = null;

		switch (direction) {
			case LEFT_VAL :
				val = cfg.indexValues.get(index).leftVal;
				break;
			case MID_VAL :
				val = cfg.indexValues.get(index).midVal;
				break;
			case RIGHT_VAL :
				val = cfg.indexValues.get(index).rightVal;
				break;
			case DEFAULT_VAL :
			default :
				val = raw;
				break;
		}
		if (val == null)
			val = raw;

		return val;
	}

	// 将原始值转为“数据库转换”。
	private static Double toDBConvert(Double src) {
		if (src == null)
			return 0d;

		return Math.pow(10, (src.doubleValue() / 10)) / 1000;
	}

	private static class Val {

		Double rawVal; // 原始值

		Double uwayVal;// uway采样点数

		// 左中右值。
		Double leftVal;

		Double midVal;

		Double rightVal;

		public Val(Double rawVal, Double uwayVal, Double leftVal, Double midVal, Double rightVal) {
			super();
			this.rawVal = rawVal;
			this.uwayVal = uwayVal;
			this.leftVal = leftVal;
			this.midVal = midVal;
			this.rightVal = rightVal;
		}

		@Override
		public String toString() {
			return "Val [rawVal=" + rawVal + ", uwayVal=" + uwayVal + ", leftVal=" + leftVal + ", midVal=" + midVal + ", rightVal=" + rightVal + "]";
		}

	}

	/**
	 * 根据表名、原始counter名、列名，来判断是否需要进行计算。
	 * 
	 * @param tabName
	 *            表名。
	 * @param counterName
	 *            原始counter名。
	 * @param colName
	 *            列名。
	 * @return 如果需要计算，返回counter组计算信息，否则返回<code>null</code>.
	 */
	public static CalCfgItem isNeedCal(String tabName, /* String counterName, */String colName) {
		if (Util.isNull(tabName) /* || Util.isNull(counterName) */
				|| Util.isNull(colName))
			return null;

		for (CalCfgItem cc : ITEMS) {
			if (cc.getTabName().equalsIgnoreCase(tabName)
			/* && cc.getCounterName().equalsIgnoreCase(counterName) */
			&& cc.getColName().equalsIgnoreCase(colName))
				return cc;
		}

		return null;
	}

	/**
	 * 找出一个表中所有要计算的counter.
	 * 
	 * @param tabName
	 *            表名。
	 * @return 所有要计算的counter，不返回<code>null</code>.
	 */
	public static List<CalCfgItem> findNeedCal(String tabName) {
		List<CalCfgItem> result = new ArrayList<CalCfgItem>();

		if (Util.isNull(tabName))
			return result;

		for (CalCfgItem cc : ITEMS) {
			if ((cc.getTabName().equalsIgnoreCase(tabName)))
				result.add(cc);
		}

		return result;
	}

	private static final int LEFT_VAL = 0;

	private static final int MID_VAL = 1;

	private static final int RIGHT_VAL = 2;

	private static final int DEFAULT_VAL = 3;

	// clt_pm_w_eric_cal_cfg的一条记录，保存一个counter组的列名，包括入到哪个列，在哪个表，属于第几组等。
	public static class CalCfgItem {

		String counterName;

		String colName;

		String tabName;

		int counterGroup;

		String additionColMax;

		String additionColAvg;

		int maxDirection;

		int avgDirection;

		int calType;

		public CalCfgItem(String counterName, String colName, String tabName, int counterGroup, String additionColMax, String additionColAvg,
				int maxDirection, int avgDirection, int calType) {
			super();
			this.counterName = counterName;
			this.colName = colName;
			this.tabName = tabName;
			this.counterGroup = counterGroup;
			this.additionColMax = additionColMax;
			this.additionColAvg = additionColAvg;
			this.maxDirection = maxDirection;
			this.avgDirection = avgDirection;
			this.calType = calType;
		}

		public String getCounterName() {
			return counterName;
		}

		public String getColName() {
			return colName;
		}

		public String getTabName() {
			return tabName;
		}

		public int getCounterGroup() {
			return counterGroup;
		}

		public String getAdditionColMax() {
			return additionColMax;
		}

		public String getAdditionColAvg() {
			return additionColAvg;
		}

		public int getMaxDirection() {
			return maxDirection;
		}

		public int getAvgDirection() {
			return avgDirection;
		}

		/**
		 * @return the calType
		 */
		public int getCalType() {
			return calType;
		}

		@Override
		public String toString() {
			return "CalCfgItem [counterName=" + counterName + ", colName=" + colName + ", tabName=" + tabName + ", counterGroup=" + counterGroup
					+ ", additionColMax=" + additionColMax + ", additionColAvg=" + additionColAvg + ", maxDirection=" + maxDirection
					+ ", avgDirection=" + avgDirection + ",calType=" + calType + "]";
		}

	}

	// 存放UWAY采样点配置表数据，针对单个COUNTER组
	static class CalVals {

		int counterGroup;

		Map<Integer, Val> indexValues = new HashMap<Integer, Val>();

		public CalVals(int counterGroup) {
			super();
			this.counterGroup = counterGroup;
		}

		public int getCounterGroup() {
			return counterGroup;
		}

		@Override
		public String toString() {
			return "CalCfg [counterGroup=" + counterGroup + ", indexValues=" + indexValues + "]";
		}

	}

	public static void main(String[] args) {
		// System.out.println(DBFunction.isNeedCal("CLT_PM_W_ERIC_HSDSCHRESOURCES",
		// "PMREPORTEDCQI"));

		// static final int NORMAL = 0;// 正常
		// static final int ALL_ZERO = 1; // 全都是0
		// static final int ALL_NULL = 2; // 全都是null
		// static final int ZERO_NULL = 3; // 有0，有null
		// static final int NULL_NORMAL = 4; // 有非零值，有null
		// System.out.println(getCountersState(parseCounterList(",,,1")));

		// String result = "";
		// int i = 0;
		// while (result.length() != 5)
		// {
		// result = "30101141".substring(i++, "30101141".length());
		// }
		// System.out.println(result);
		//
		// System.out.println(toDBConvert(50.00));
		// System.out.println(DBFunction.CFG);

	}
}
