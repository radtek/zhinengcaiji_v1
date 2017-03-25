//采集任务生命周期详情。
$(document)
		.ready(
				function() {
					$("#btnBack").click(function() {
						window.history.back();
					});
					$("#btnRefresh").click(function() {
						window.location.reload(true);
					});

					$("#btnQuery").focus();

					$("#txtDay").select();

					$("#finishedTasks").find("tr").each(function(i) {
						if (i > 0) {
							$(this).hide();
						}
					});
					$("#showFinished")
							.click(
									function() {
										if (trimString($("#showFinished")
												.text()) == "\u663e\u793a") {
											$("#finishedTasks")
													.find("tr")
													.each(
															function(i) {
																if (i > 0) {
																	$(this)
																			.show(
																					"fast");
																}
															});
											$("#showFinished").text(
													"\u9690\u85cf");
										} else {
											$("#finishedTasks")
													.find("tr")
													.each(
															function(i) {
																if (i > 0) {
																	$(this)
																			.hide(
																					"fast");
																}
															});
											$("#showFinished").text(
													"\u663e\u793a");
										}
									});
					$("#showRunning")
							.click(
									function() {
										if (trimString($("#showRunning").text()) == "\u663e\u793a") {
											$("#runningTasks")
													.find("tr")
													.each(
															function(i) {
																if (i > 0) {
																	$(this)
																			.show(
																					"fast");
																}
															});
											$("#showRunning").text(
													"\u9690\u85cf");
										} else {
											$("#runningTasks")
													.find("tr")
													.each(
															function(i) {
																if (i > 0) {
																	$(this)
																			.hide(
																					"fast");
																}
															});
											$("#showRunning").text(
													"\u663e\u793a");
										}
									});
					$("#showAwaiting")
							.click(
									function() {
										if (trimString($("#showAwaiting")
												.text()) == "\u663e\u793a") {
											$("#awaitingTasks")
													.find("tr")
													.each(
															function(i) {
																if (i > 0) {
																	$(this)
																			.show(
																					"fast");
																}
															});
											$("#showAwaiting").text(
													"\u9690\u85cf");
										} else {
											$("#awaitingTasks")
													.find("tr")
													.each(
															function(i) {
																if (i > 0) {
																	$(this)
																			.hide(
																					"fast");
																}
															});
											$("#showAwaiting").text(
													"\u663e\u793a");
										}
									});
					$("#btnQuery")
							.click(
									function() {
										var day = $("#txtDay").val();
										day = trimString(day);
										if (day.length > 0 && !isNaN(day)) {
											if (day.indexOf(".") >= 0) {
												$("#txtDay").focus();
												$("#txtDay").select();
											} else {
												window.location
														.replace("/page/task/taskLifeCycleServlet?day="
																+ day
																+ "&taskId="
																+ $("#taskId")
																		.val());
											}
										} else {
											$("#txtDay").focus();
											$("#txtDay").select();
										}
									});
					$("#txtDay")
							.keypress(
									function(event) {
										if (event.keyCode == 13) {
											var day = $("#txtDay").val();
											day = trimString(day);
											if (day.length > 0 && !isNaN(day)) {
												if (day.indexOf(".") >= 0) {
													$("#txtDay").focus();
													$("#txtDay").select();
												} else {
													window.location
															.replace("/page/task/taskLifeCycleServlet?day="
																	+ day
																	+ "&taskId="
																	+ $(
																			"#taskId")
																			.val());
												}
											} else {
												$("#txtDay").focus();
												$("#txtDay").select();
											}
										}
									});
				});

function trimString(text) {
	return (text || "").replace(/^\s+|\s+$/g, "");
}