package parser;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import store.AbstractStore;
import store.StoreFactory;
import templet.DBAutoTempletP2;
import templet.Table;
import templet.Table.Column;
import util.CommonDB;
import util.Util;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 数据库接入方式，无需配置模块(解析、分发模板在一起)，解析华为和中兴的数据
 * 
 * @author 尹海龙 2012-2-4
 * @since 1.0
 * @see DBAutoParser2
 * @see DBAutoParser3
 */
public class DBAutoParser3 extends DBAutoParser2 {

	private Map<String, Map<String, Object>> mappingspecial; // 模板

	private org.w3c.dom.Node node; // 节点

	private List<String> listdest = new ArrayList<String>(); // 添加表中的有效字段

	private List<String> listpara = new ArrayList<String>(); // 拆解字段中包含的 有效字段

	private String[] fieldarray = null; // 华为数据

	private String checkIndex = null; // 华为数据分隔符

	private SimpleDateFormat dfAll = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private SimpleDateFormat dfDay = new SimpleDateFormat("yyyy-MM-dd");

	private String PARA_NAME = "DBAutoParser3_PARA_NAME";

	private String datatype_number = "DBAutoParser3_datatype_number";

	private String removeNoise(int colType, String colVal) {
		if (colVal == null)
			return "";
		if (colType == 3) {
			colVal = colVal.toUpperCase().replace("Y", "").replace("M", "-").replace("D", "-").replace("H", " ").replace("N", ":").replace("S", ":");
			colVal = colVal.trim();
			if (colVal.length() <= 10) {
				try {
					colVal = dfDay.format(dfDay.parse(colVal)).toString();
				} catch (ParseException e) {
				}
				return colVal + " 00:00:00";
			}
			try {
				colVal = dfAll.format(dfAll.parse(colVal)).toString();
			} catch (ParseException e) {
			}
			return colVal;
		}
		// 字段中不能出现以下字符
		colVal = colVal.trim().replaceAll(";", " ").replaceAll("\r\n", " ");
		colVal = colVal.replaceAll("\n", " ").replaceAll("\r", " ");

		return colVal;
	}

	public int parseData(ResultSet rs, DBAutoTempletP2.Templet temP) throws Exception {
		int recordCount = 0;

		AbstractStore sqlldrStore = null;

		// 获取此模板所有的字段映射
		DBAutoTempletP2 temp = (DBAutoTempletP2) collectObjInfo.getParseTemplet();
		mappingspecial = temp.getMappingspecial();

		defaultValueColumns = temP.getDefaultValueColumns();

		// 获取采集表结构
		String desTable = temP.getDestTableName();
		String locsql = toSql(desTable, null);
		locsql += " where 1=2";
		Connection destConn = null;
		PreparedStatement descPs = null;
		ResultSet destRs = null;
		try {
			destConn = CommonDB.getConnection();
			descPs = destConn.prepareStatement(locsql);
			destRs = descPs.executeQuery();

			// 厂家是否是oracle数据库
			boolean isOracle = collectObjInfo.getDBDriver().contains("oracle");

			ResultSetMetaData srcMeta = rs.getMetaData(); // 厂家表结构
			ResultSetMetaData destMeta = destRs.getMetaData(); // 采集表结构

			Table table = new Table();
			table.setName(temP.getDestTableName());
			table.setId(temP.getId());
			table.setSplitSign(DELIMIT);
			// 解析元数据
			parseMeta(srcMeta, destMeta, table);
			// 处理数据
			Collection<Column> colection = table.getColumns().values();
			// System.out.println(colection.size());
			while (rs.next()) {
				List<Object> listTemp = new ArrayList<Object>();
				int countField = 0;
				Map<Integer, String> map = new HashMap<Integer, String>();
				for (Column col : colection) {
					int extIndex = col.getExtIndex();

					String colVal = null;

					// 厂家表没有该字段,默认索引为0
					if (extIndex == 0) {
						listTemp.add(defaultValueValidate(col, ""));
						continue;
					}

					if (col.getType() != 4)// 采集表字段类型，大字段类型2005映射為4
					{
						if (map.containsKey(extIndex)) {
							colVal = map.get(extIndex);
						} else {
							Object o = rs.getObject(extIndex);

							colVal = o == null ? "" : o.toString();// 列值
							map.put(extIndex, colVal);
						}

						colVal = defaultValueValidate(col, colVal);

						if (!this.listdest.contains(col.getName())) {
							listTemp.add(removeNoise(col.getType(), colVal));
						} else {
							int tempcount = putSpecial(listTemp, colVal, col.getName());
							countField = countField > tempcount ? countField : tempcount;
						}
					} else {
						if (isOracle) {
							colVal = ConstDef.ClobParse(rs.getClob(extIndex));
						} else {
							if (map.containsKey(extIndex)) {
								colVal = map.get(extIndex);
							} else {
								Object o = rs.getObject(extIndex);

								colVal = o == null ? "" : o.toString();// 列值
								map.put(extIndex, colVal);
							}
						}

						colVal = defaultValueValidate(col, colVal);

						File clob = new File(SystemConfig.getInstance().getCurrentPath(), "clob_" + collectObjInfo.getTaskID() + "_"
								+ Util.getDateString_yyyyMMddHHmmssSSS(collectObjInfo.getLastCollectTime()) + "_" + (clobIndex++) + ".clob");
						PrintWriter pw = new PrintWriter(clob);
						pw.print(colVal == null ? "" : colVal);
						pw.flush();
						pw.close();
						if (!this.listdest.contains(col.getName())) {
							listTemp.add(clob.getAbsolutePath());
						} else {
							int tempcount = putSpecial(listTemp, colVal, col.getName());
							countField = countField > tempcount ? countField : tempcount;
						}
						clobFiles.add(clob);
					}
				}

				// 生成SQLldr有效格式
				for (int i = 0; i < countField; i++) {
					StringBuilder colVals = new StringBuilder();
					boolean isLode = true;
					for (int j = 0; j < listTemp.size(); j++) {
						Object listVal = listTemp.get(j);
						if (listVal instanceof String) {
							if (listVal.toString().equalsIgnoreCase(this.datatype_number)) {
								isLode = false;
								continue;
							}
							colVals.append(listVal).append(DELIMIT);
						} else if (listVal instanceof String[]) {
							String[] tt = (String[]) listVal;
							if (tt.length >= 1 && tt[tt.length - 1].equalsIgnoreCase(this.PARA_NAME)) {
								isLode = false;
								continue;
							}
							colVals.append(tt.length == 0 ? "" : tt[i]).append(DELIMIT);
						} else if (listVal instanceof Object[]) {
							Object[] tt = (Object[]) listVal;
							if (tt.length >= 1 && tt[tt.length - 1].toString().equalsIgnoreCase(this.PARA_NAME)) {
								isLode = false;
								continue;
							}
							colVals.append(tt.length == 0 ? "" : tt[i]).append(DELIMIT);
						}
					}
					if (isLode) {
						++recordCount;
						// System.out.println(colVals);
						// distribute(colVals.toString(), temP.getId(), table,sqlldrStore);

						if (sqlldrStore == null) {
							sqlldrStore = StoreFactory.getStore(temP.getId(), table, this.collectObjInfo,dataTime);
							sqlldrStore.open();
						}
						sqlldrStore.write(colVals.toString());
					}
				}
				map.clear();
				listpara.clear();
			}
		} catch (Exception e) {
			log.error("taskid = " + collectObjInfo + " , 解析出现异常,原因：{}", e);
		} finally {
			commit(sqlldrStore);
			CommonDB.close(destRs, descPs, destConn);
		}

		return recordCount;
	}

	/**
	 * 解析拆解字段数据
	 * 
	 * @param listTemp
	 * @param colVal
	 * @param colName
	 * @return
	 */
	private int putSpecial(List<Object> listTemp, String colVal, String colName) {
		// System.out.println(colName);
		int count = 1;
		Node split = node.getAttributes().getNamedItem("split");

		boolean isnull = true;

		if (null != split && listpara.size() == 0) {

			if (node.getAttributes().getNamedItem("substr") != null) {
				colVal = colVal.substring(colVal.indexOf(node.getAttributes().getNamedItem("substr").getNodeValue()) + 1);
			}
			String[] para = colVal.split(split.getNodeValue());
			int length = para.length;
			for (int i = 0; i < length; i++) {
				listpara.add(para[i].trim());
			}
		}

		// 华为 分行数据
		if (check(colName)) {
			isnull = false;
			List<String> listValue = new ArrayList<String>();
			if (checkIndex != null && fieldarray != null) {

				int max = 0;
				for (String s : fieldarray) {
					for (int i = 0; i < listpara.size(); i++) {
						if (listpara.get(i).toUpperCase().startsWith(s.toUpperCase())) {
							max = i > max ? i : max;
							break;
						}
					}
				}

				List<String> list = listpara.subList(max + 1, listpara.size());

				boolean isNull = false;
				for (String s : list) {
					String[] tt = s.split(checkIndex);
					// CN、SCTID、CRRID最后一个数值不为空开始，依次解析每一个参数，每个参数独立一行数据
					if (tt.length == 2 && tt[1].trim().length() >= 1 && !isNull) {
						isNull = true;
					}
					if (isNull) {
						if (colName.equalsIgnoreCase("PARA_NAME")) {
							listValue.add(tt[0].trim());
						} else if (colName.equalsIgnoreCase("PARA_VALUE")) {
							String tttt = tt[1].trim();
							tttt = tttt.endsWith(";") ? tttt.substring(0, tttt.length() - 1) : tttt;
							if (tttt.startsWith("\""))
								tttt = tttt.substring(1);
							if (tttt.endsWith("\""))
								tttt = tttt.substring(0, tttt.length() - 1);
							listValue.add(tttt);
						}
					}
				}
			}
			count = listValue.size();
			listTemp.add(listValue.toArray());
		}

		// 正常拆分字段数据
		for (int i = 0; i < listpara.size(); i++) {
			{
				String paraval = listpara.get(i);
				if (node.getChildNodes() != null) {
					org.w3c.dom.NodeList childs = node.getChildNodes();
					for (int j = 0; j < childs.getLength(); j++) {
						Node n = childs.item(j);
						if (n.getNodeName().equalsIgnoreCase("col")) {
							Node Field = n.getAttributes().getNamedItem("src");

							// 修改值Field，分行
							if ((null != Field && Field.getNodeValue().equalsIgnoreCase("Field"))
									|| (null != Field && Field.getNodeValue().equalsIgnoreCase("check"))) {
								// 中兴数据类型
								if (paraval.indexOf("Field") != -1
										&& (colName.equalsIgnoreCase("PARA_NAME") || colName.equalsIgnoreCase("modValue") || colName
												.equalsIgnoreCase("modedValue"))) {
									String substr = n.getAttributes().getNamedItem("substr").getNodeValue();
									String splitfield = n.getAttributes().getNamedItem("split").getNodeValue();
									String PARA_NAME = n.getAttributes().getNamedItem("field").getNodeValue();
									String modedValue = n.getAttributes().getNamedItem("modedValue").getNodeValue();
									String modValue = n.getAttributes().getNamedItem("modValue").getNodeValue();
									String index = "";
									NodeList field = n.getChildNodes();
									for (int f = 0; f < field.getLength(); f++) {
										Node sonfield = field.item(f);
										if (sonfield.getNodeName().equalsIgnoreCase("Field")) {
											String pindex = sonfield.getAttributes().getNamedItem("index").getNodeValue();
											if (paraval.indexOf(pindex) != -1) {
												index = pindex;
												break;
											}
										}
									}

									paraval = paraval.substring(paraval.indexOf(substr) + 1);
									String[] tempmod = paraval.split(splitfield);
									count = tempmod.length;
									// Field 字段
									if (colName.equalsIgnoreCase(PARA_NAME)) {
										for (int t = 0; t < tempmod.length; t++) {
											if (tempmod[t].indexOf(substr) != -1) {
												tempmod[t] = tempmod[t].substring(0, tempmod[t].indexOf(substr));
											} else {
												tempmod[t] = tempmod[t].substring(0, tempmod[t].indexOf(index));
											}
										}
									}
									// modValue 修改前得值,没有为空
									else if (colName.equalsIgnoreCase(modValue)) {
										for (int t = 0; t < tempmod.length; t++) {
											if (tempmod[t].indexOf(substr) != -1) {
												tempmod[t] = tempmod[t].substring(tempmod[t].indexOf(substr) + 1).split(index)[0];
											} else {
												tempmod[t] = "";
											}

										}
									}
									// modedValue 修改后的值,没有为空
									else if (colName.equalsIgnoreCase(modedValue)) {
										for (int t = 0; t < tempmod.length; t++) {
											if (tempmod[t].indexOf(substr) != -1) {
												tempmod[t] = tempmod[t].split(index)[1];
											} else {
												tempmod[t] = tempmod[t].substring(tempmod[t].indexOf(index) + 1);
											}

										}
									}
									listTemp.add(tempmod);
									isnull = false;
								}
							}
							// 节点colson中映射的字段
							else {
								if (null != n.getAttributes().getNamedItem("substr")) {
									String substr = n.getAttributes().getNamedItem("substr").getNodeValue();
									String splist = n.getAttributes().getNamedItem("split").getNodeValue();

									String subStrVal = paraval.substring(paraval.indexOf(substr) + 1).trim();
									if (subStrVal.startsWith("{")) {
										subStrVal = subStrVal.substring(1);
									}
									if (subStrVal.endsWith("}")) {
										subStrVal = subStrVal.substring(0, subStrVal.length() - 1);
									}

									String[] sonSplits = subStrVal.split(splist);
									//
									for (String s : sonSplits) {
										org.w3c.dom.NodeList colson = n.getChildNodes();
										for (int k = 0; k < colson.getLength(); k++) {
											Node son = colson.item(k);
											// 迭代有效的子节点
											if (son.getNodeName().equalsIgnoreCase("colson")) {
												if (colName.equalsIgnoreCase(son.getAttributes().getNamedItem("dest").getNodeValue())) {

													String sonsplit = son.getAttributes().getNamedItem("split").getNodeValue();

													String srcor = son.getAttributes().getNamedItem("srcor") == null ? "" : son.getAttributes()
															.getNamedItem("srcor").getNodeValue();
													String src = son.getAttributes().getNamedItem("src").getNodeValue();
													// 数据合法性校验
													if ((s.indexOf(src) != -1 || (srcor.length() >= 1 && s.indexOf(srcor) != -1))
															&& s.indexOf(sonsplit) != -1) {
														String[] splitVal = s.split(sonsplit);
														if (splitVal.length == 2) {
															String tttt = splitVal[1].trim();
															if (tttt.startsWith("\""))
																tttt = tttt.substring(1);
															if (tttt.endsWith("\""))
																tttt = tttt.substring(0, tttt.length() - 1);
															listTemp.add(tttt);
														}

														isnull = false;
														break;
													}
												}
											}
										}
										if (!isnull) {
											break;
										}
									}
								}
								// 几点 col 中 映射的字段
								else {
									if (colName.equalsIgnoreCase(n.getAttributes().getNamedItem("dest").getNodeValue())) {
										String colsplit = n.getAttributes().getNamedItem("split").getNodeValue();
										Node src = n.getAttributes().getNamedItem("src");
										Node datatype = n.getAttributes().getNamedItem("datatype");
										if (colsplit.trim().length() == 0 && src == null && i == 0) {
											if (datatype != null && datatype.getNodeValue().equalsIgnoreCase("number")) {
												try {
													Long.parseLong(paraval);
													listTemp.add(paraval);
												} catch (Exception e) {
													listTemp.add(this.datatype_number);
												}
											} else
												listTemp.add(paraval);
											// listpara.remove(i);
											isnull = false;
										} else if (src != null && paraval.toLowerCase().indexOf(src.getNodeValue().toLowerCase()) != -1) {
											String[] splitVal = paraval.split(colsplit);
											if (splitVal.length == 2) {
												String tttt = splitVal[1].trim();
												if (tttt.startsWith("\""))
													tttt = tttt.substring(1);
												if (tttt.endsWith("\""))
													tttt = tttt.substring(0, tttt.length() - 1);
												if (datatype != null && datatype.getNodeValue().equalsIgnoreCase("number")) {
													try {
														Long.parseLong(tttt);
														listTemp.add(tttt);
													} catch (Exception e) {
														listTemp.add(this.datatype_number);
													}
												} else
													listTemp.add(tttt);
											}
											// listpara.remove(i);
											isnull = false;
										}
										break;
									}
								}
							}
						}

						if (!isnull) {
							break;
						}
					}
				}

				if (!isnull) {
					break;
				}
			}
		}
		if (isnull)
			if (colName.equalsIgnoreCase("PARA_NAME"))
				listTemp.add(new String[]{this.PARA_NAME});
			else
				listTemp.add("");
		return count;
	}

	/**
	 * 检查是否为华为的合法数据
	 * 
	 * @param colName
	 * @return
	 */
	private boolean check(String colName) {

		boolean isOk = false;
		org.w3c.dom.NodeList childs = node.getChildNodes();

		for (int i = 0; i < childs.getLength(); i++) {
			Node n = childs.item(i);
			if (n.getNodeName().equalsIgnoreCase("col")) {
				Node Field = n.getAttributes().getNamedItem("src");
				// 是否为华为数据类型
				if (null != Field && Field.getNodeValue().equalsIgnoreCase("check")) {
					String check = n.getAttributes().getNamedItem("check").getNodeValue();
					String checksign = n.getAttributes().getNamedItem("checksign").getNodeValue();
					this.checkIndex = n.getAttributes().getNamedItem("index").getNodeValue();
					String PARA_NAME = n.getAttributes().getNamedItem("field").getNodeValue();
					String modedValue = n.getAttributes().getNamedItem("modedValue").getNodeValue();
					// 需要检查的字段
					if (colName.equalsIgnoreCase(PARA_NAME) || colName.equalsIgnoreCase(modedValue)) {
						String[] field = check.split(checksign);
						int index = 0;
						for (String fd : field) {
							for (int t = 0; t < listpara.size(); t++) {
								// 当前校验字段是否存在于数据中
								if (listpara.get(t).toUpperCase().startsWith(fd.toUpperCase())) {
									++index;
									break;
								}
							}
						}

						if (index >= 1) {
							isOk = true;
							this.fieldarray = field;
						}
					}
				}
			}
		}
		return isOk;
	}

	/**
	 * 解析添加数据的字段
	 * 
	 * @param srcMeta
	 * @param descMeta
	 * @param table
	 * @throws Exception
	 */
	private void parseMeta(ResultSetMetaData srcMeta, ResultSetMetaData descMeta, Table table) throws Exception {
		int srcColCount = srcMeta.getColumnCount(); // 获取厂家表列数
		int destColCount = descMeta.getColumnCount(); // 获取采集表列数
		String tbName = table.getName(); // 采集表名
		Map<String, Object> mcMap = null;
		List<String> listdest = new ArrayList<String>();

		int srcIndex = 0;
		if (mappingspecial.containsKey(tbName) && mappingspecial.get(tbName) != null && mappingspecial.get(tbName).size() > 0) {
			mcMap = mappingspecial.get(tbName);
			for (Object coll : mcMap.values()) {
				if (coll instanceof org.w3c.dom.Node) {
					node = (Node) coll;
					String srcname = node.getAttributes().getNamedItem("src").getNodeValue();
					for (int i = 1; i <= srcColCount; i++) {
						if (srcMeta.getColumnName(i).equalsIgnoreCase(srcname)) {
							srcIndex = i;
							break;
						}
					}
					if (node.getChildNodes() != null) {
						org.w3c.dom.NodeList childs = node.getChildNodes();
						for (int j = 1; j <= destColCount; j++) {
							String destColName = descMeta.getColumnName(j);

							for (int i = 0; i < childs.getLength(); i++) {
								Node n = childs.item(i);

								if (n.getNodeName().equalsIgnoreCase("col")) {
									Node Field = n.getAttributes().getNamedItem("src");
									// 修改值
									if ((null != Field && Field.getNodeValue().equalsIgnoreCase("Field"))
											|| (null != Field && Field.getNodeValue().equalsIgnoreCase("check"))) {
										String PARA_NAME = n.getAttributes().getNamedItem("field").getNodeValue();
										String modedValue = n.getAttributes().getNamedItem("modedValue").getNodeValue();
										Node modValue = n.getAttributes().getNamedItem("modValue");
										if (modValue != null && destColName.equalsIgnoreCase(modValue.getNodeValue())) {
											if (!listdest.contains(destColName)) {
												putColumn(destColName, j, srcIndex, descMeta, table);
												listdest.add(destColName);
												this.listdest.add(destColName);
												break;
											}
										}
										if (destColName.equalsIgnoreCase(modedValue)) {
											if (!listdest.contains(destColName)) {
												putColumn(destColName, j, srcIndex, descMeta, table);
												listdest.add(destColName);
												this.listdest.add(destColName);
												break;
											}
										}

										if (destColName.equalsIgnoreCase(PARA_NAME)) {
											if (!listdest.contains(destColName)) {
												putColumn(destColName, j, srcIndex, descMeta, table);
												listdest.add(destColName);
												this.listdest.add(destColName);
												break;
											}
										}
									}
									// 节点colson中映射的字段
									else {
										if (null != n.getAttributes().getNamedItem("substr")) {
											NodeList colson = n.getChildNodes();

											for (int k = 0; k < colson.getLength(); k++) {
												Node son = colson.item(k);
												if (son.getNodeName().equalsIgnoreCase("colson")) {

													if (destColName.equalsIgnoreCase(son.getAttributes().getNamedItem("dest").getNodeValue())) {
														if (!listdest.contains(destColName)) {
															putColumn(destColName, j, srcIndex, descMeta, table);
															listdest.add(destColName);
															this.listdest.add(destColName);
															break;
														}
													}
												}
											}
										}
										// 几点 col 中 映射的字段
										else {
											String mydestname = n.getAttributes().getNamedItem("dest").getNodeValue();
											if (destColName.equalsIgnoreCase(mydestname) || ("\"" + destColName + "\"").equalsIgnoreCase(mydestname)) {
												if (!listdest.contains(mydestname)) {
													putColumn(mydestname, j, srcIndex, descMeta, table);
													listdest.add(mydestname);
													this.listdest.add(mydestname);
													break;
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}

		// 注意: 元数据下标从1开始
		for (int j = 1; j <= destColCount; j++) {
			String destColName = descMeta.getColumnName(j);

			// 系统字段跳过
			if (destColName.equalsIgnoreCase("OMCID") || destColName.equalsIgnoreCase("COLLECTTIME") || destColName.equalsIgnoreCase("STAMPTIME"))
				continue;
			int c = 0;
			for (int i = 1; i <= srcColCount; i++) {

				String srcColName = srcMeta.getColumnName(i);// 厂家表字段名称
				// 包含映射的情况
				if (mcMap != null && mcMap.containsKey(destColName) && mcMap.get(destColName) instanceof String
						&& mcMap.get(destColName).toString().equalsIgnoreCase(srcColName)) {
					if (!listdest.contains(destColName)) {
						putColumn(destColName, j, i, descMeta, table);
						listdest.add(destColName);
					}
					break;

				}
				// 不包含映射的情况
				if (srcColName.equalsIgnoreCase(destColName)) {
					if (!listdest.contains(destColName)) {
						putColumn(destColName, j, i, descMeta, table);
						listdest.add(destColName);
					}
					break;
				}
				c++;
			}
			// 厂家表没有该字段,设置索引为0
			if (c == srcColCount) {
				putColumn(destColName, j, 0, descMeta, table);
			}
		}

		// System.out.println(listdest.size());
	}
}
