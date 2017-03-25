$(document)
		.ready(function() {
			if ($("#pageCount").val() == "") {
				$("#sumForm").submit();
			}
			// 点上一页链接
				$("#prePage").click(function() {
					var currPage = parseInt($("#currentPage").val());
					if (currPage > 1) {
						$("#currentPage").val(currPage - 1);
						$("#sumForm").submit();
					}
				});

				// 点下一页链接
				$("#nextPage").click(function() {
					var currPage = parseInt($("#currentPage").val());
					var max = parseInt($("#pageCount").val());
					if (currPage < max) {
						$("#currentPage").val(currPage + 1);
						$("#sumForm").submit();
					}
				});

				// 点首页链接
				$("#firstPage").click(function() {
					$("#currentPage").val("1");
					$("#sumForm").submit();
				});
				// 点末页链接
				$("#lastPage").click(function() {
					$("#currentPage").val($("#pageCount").val());
					$("#sumForm").submit();
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
									$("#currentPage").val(str);
									$("#sumForm").submit();
								});
				// 条件查询
				$("#query")
						.click(
								function() {
									var type = $("#selectType").val();
									var val = $("#selectKeyWord").val();
									if (type == "") {
										return;
									}
									var obj = $("<input type=\"hidden\" name=\""
											+ type
											+ "\" value=\""
											+ val
											+ "\" />");
									var adv = $("<input type=\"hidden\" name=\"adv\" />");
									$("#sumForm").append(obj);
									$("#sumForm").append(adv);
									$("#sumForm").submit();
								});
			});