$(document)
		.ready(function() {
			// 页面加载时，显示第一页数据
				if ($("#pageCount").val() == "") {
					var queryType = $("#queryType").val();
					if (queryType == "0") {
						$("#clearCondition").val("0");
						$("#action").val("list");
						$("#currentPage").val("1");
						$("#taskForm").submit();
					}
				}

				// 点击查询按钮
				$("#query").click(function() {
					var queryType = $("#queryType").val();
					if (queryType == "0") {
						$("#action").val("list");
						$("#currentPage").val("1");
						$("#clearCondition").val("0");
						$("#taskForm").submit();
					} else if ($("#queryValue").val() != "") {
						$("#currentPage").val("1");
						var obj = $("<input type=\"hidden\" />");
						obj.attr("name", queryType);
						obj.val($("#queryValue").val());
						if (obj.attr("name") != "taskId") {
							$("#taskForm").append(obj);
						} else {
							$("#taskId").val($("#queryValue").val());
						}
						$("#action").val("query");
						$("#taskForm").submit();
					}

				});
				// 点上一页链接
				$("#prePage").click(function() {
					var currPage = parseInt($("#currentPage").val());
					if (currPage > 1) {
						$("#action").val("list");
						$("#currentPage").val(currPage - 1);
						$("#taskForm").submit();
					}
				});

				// 点下一页链接
				$("#nextPage").click(function() {
					var currPage = parseInt($("#currentPage").val());
					var max = parseInt($("#pageCount").val());
					if (currPage < max) {
						$("#action").val("list");
						$("#currentPage").val(currPage + 1);
						$("#taskForm").submit();
					}
				});

				// 点首页链接
				$("#firstPage").click(function() {
					$("#action").val("list");
					$("#currentPage").val("1");
					$("#taskForm").submit();
				});
				// 点末页链接
				$("#lastPage").click(function() {
					$("#action").val("list");
					$("#currentPage").val($("#pageCount").val());
					$("#taskForm").submit();
				});
				// 点go链接
				$("#go")
						.click(
								function() {
									var str = $("#inputPage").val();
									if (str == "") {
										return;
									}
									if (isNaN(str)) {
										alert("\u8f93\u5165\u7684\u9875\u6570\u4e0d\u6b63\u786e");
										$("#inputPage").focus();
										return;
									}
									if (parseInt(str) < 1
											|| parseInt(str) > parseInt($(
													"#pageCount").val())) {
										alert("\u9875\u6570\u5e94\u57281\u5230"
												+ $("#pageCount").val()
												+ "\u4e4b\u524d");
										$("#inputPage").focus();
										return;
									}
									$("#action").val("list");
									$("#currentPage").val(str);
									$("#taskForm").submit();
								});
				// 全选/全不选
				$("#selAll").click(function() {
					var s = $("#selAll");
					$("#taskForm").find("input").each(function() {
						if ($(this).attr("type") == "checkbox") {
							$(this).attr("checked", s.attr("checked"));
						}
					});
				});
				// 删除任务按钮
				$("#delTask")
						.click(
								function() {
									var str = "";
									$("tr[id]")
											.each(
													function() {
														if ($(this)
																.find(
																		"input:first")
																.attr("checked")) {
															str += ($(this)
																	.attr("id") + ",");
														}
													});
									if (str != "") {
										if (confirm("\u786e\u8ba4\u8981\u5220\u9664\u6240\u9009\u4efb\u52a1\u5417\uff1f")) {
											$("#delFlag").val(str);
											$("#action").val("delTask");
											$("#taskForm").submit();
										}
									}
								});
				// 添加任务按钮
				$("#addTask").click(
						function() {
							var taskId = "";
							$("tr[id]").each(
									function() {
										if ($(this).find("input:first").attr(
												"checked")
												&& taskId == "") {
											taskId = $(this).attr("id");
										}
									});
							if (taskId == "") {
								window.location.replace("/page/addTask.jsp");
							} else {
								$("#taskId").val(taskId);
								$("#action").val("toAdd");
								$("#taskForm").submit();
							}
						});
				// 转到高级查询页面
				$("#advQuery").click(function() {
					window.location.replace("/page/taskQuery.jsp");
				});
				// 本机任务
				$("#local").click(function() {
					$("#currentPage").val("1");
					var obj = $("<input type=\"hidden\" />");
					obj.attr("name", "collectorName");
					obj.val($("#collector_name").val());
					$("#taskForm").append(obj);
					$("#action").val("query");
					$("#clearCondition").val("1");
					$("#taskForm").submit();
				});
				// 有效任务
				$("#usedTaskBtn").click(function() {
					$("#currentPage").val("1");
					var obj = $("<input type=\"hidden\" />");
					obj.attr("name", "isUsed");
					obj.val("1");
					$("#taskForm").append(obj);
					$("#action").val("query");
					$("#clearCondition").val("1");
					$("#taskForm").submit();
				});

				// 点击查询生命周期按钮
				$("#taskLifeCycleQuery")
						.click(
								function() {
									var taskId = $("#taskLifeCycleTaskId")
											.val();
									taskId = trimString(taskId);
									if (taskId.length > 0 && !isNaN(taskId)
											&& taskId.indexOf(".") < 0) {
										window.location
												.replace("/page/task/taskLifeCycleServlet?taskId="
														+ taskId);
									} else {
										$("#taskLifeCycleTaskId").focus();
										$("#taskLifeCycleTaskId").select();
									}
								});

				// 在生命周期按钮上的文本框按下回车
				$("#taskLifeCycleTaskId")
						.keypress(
								function(event) {
									if (event.keyCode == 13) {
										var taskId = $("#taskLifeCycleTaskId")
												.val();
										taskId = trimString(taskId);
										if (taskId.length > 0 && !isNaN(taskId)
												&& taskId.indexOf(".") < 0) {
											window.location
													.replace("/page/task/taskLifeCycleServlet?taskId="
															+ taskId);
										} else {
											$("#taskLifeCycleTaskId").focus();
											$("#taskLifeCycleTaskId").select();
										}
									}
								});
			});

function trimString(text) {
	return (text || "").replace(/^\s+|\s+$/g, "");
}
