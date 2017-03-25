$(document).ready(function(){
	if($('#totalPageLabel').text() == ""){
		subQuery(1);
	};
	$("#mainTab tr").slice(8,$(this).length-3).each(function(){
		$(this).mouseover(function(){
			$(this).addClass("sel");
		}).mouseout(function(){
			$(this).removeClass("sel");
		})
	});
	//查询按钮，各种查询
	$("#query").click(function(){
		subQuery(1);
	});
	//重置按钮
	$("#reset").click(function(){
		resetVal();
		$("#pageSize").val("");
	});
	function resetVal(){
		$("#sqlSelect").val("");
		$("#sqlCondition").text("");
		$("#qId").val("");
		$("#taskId").val("");
		$("#collector_name").val("");
		$("#readoptType").val("");
		$("#collectTime").val("");
		$("#collectStatus").val("");
		$("#collect_type").val("");
		$("#collect_period").val("");
	}
	//首页
	$('#firstPage').click(function(){
		var currPage = Number($('#firPageLabel').text());
		if(currPage<=1){
			alert("已经在首页.");
			return;
		}
		subQuery(1);
	});
	//上一页
	$('#frontPage').click(function(){
		var currPage = Number($('#firPageLabel').text());
		if(currPage<=1){
			alert("已经是首页了！");
			return;
		}
		subQuery(currPage-1);
	});
	//下一页
	$('#nextPage').click(function(){
		var currPage = Number($('#firPageLabel').text());
		var totalPage = Number($('#totalPageLabel').text());
		if(currPage==totalPage){
			alert("已经是末页了！");
			return;
		}
		subQuery(currPage+1);
	});
	//末页
	$('#lastPage').click(function(){
		var currPage = Number($('#firPageLabel').text());
		var totalPage = Number($('#totalPageLabel').text());
		if(currPage==totalPage){
			alert("已经是末页了！");
			return;
		}
		subQuery(totalPage);
	});
	//当前页
	$('#currentPage').click(function(){
		var obj = $('#pageNum');
		var currentNum = obj.val();
		var currPage = Number($('#firPageLabel').text());
		var totalPage = Number($('#totalPageLabel').text());
		if(isNaN(currentNum)||currentNum==""){
			alert("请输入数字");
			obj.focus();
			return;
		}else if(currentNum == currPage){
			alert("已经在此页了！");
			obj.focus();
			return;
		}else if(currentNum<1||currentNum>totalPage){
			alert("数字只能在1-"+totalPage+"之间.");
			obj.focus();
			return;
		}
		subQuery(currentNum);
	});
	/*requestPage 要请求的页数*/
	function subQuery(requestPage){
		$('#action').val('query');
		$('#forwardURL').val('/page/rTask.jsp');	
		$('#returnURL').val('javascript:history.back();');
		$('#page').val(requestPage);
		document.form.submit();
	}
	
	/*sql语句查询*/
	$("#sqlSelect").change(function(){
		$("#sqlCondition").val($(this).val());
	});
	//本机任务
	$("#local").click(function(){
		//$("#reset").triggerHandler("click");
		resetVal();
		$("#collector_name").val($("#hostName").val());
		subQuery(1);
	});
	//新增任务
	$("#newRtask").click(function(){
		resetVal();
		$("#collectStatus").val("0");
		subQuery(1);
	});
	//历吏任务
	$("#hisRtask").click(function(){
		resetVal();
		$("#collectStatus").val("3");
		subQuery(1);
	});
	//失效任务
	$("#invaliRtask").click(function(){
		resetVal();
		$("#collectStatus").val("-1");
		subQuery(1);
	});
	//更新按钮
	$(".uptSingle").click(function(){
		var tr = $(this).parent().parent();
		$('#action').val('get');
		$('#forwardURL').val('/page/rTaskUpdate.jsp');
		$('#returnURL').val('javascript:history.back();');
		$('#id').val(tr.find("td").eq(1).text());
		document.form.submit();
	});
	//激活按钮
	$(".actSingle").click(function(){
		if(window.confirm("确定激活此任务吗？")){
			var tr = $(this).parent().parent();
			var tid = tr.find("td").eq(2).text();
			var fp = tr.find("td").eq(3).find("textarea").text(); 
			var ct = tr.find("td").eq(4).text();  
			var cn = tr.find("td").eq(6).text();
			var rt = tr.find("td").eq(7).text();  
			var cs = tr.find("td").eq(10).find("textarea").text();
			window.location.href="/RTaskServlet?action=add&forwardURL=/page/result.jsp&returnURL=javascript:history.back();&taskID="+tid+"&filePath="+fp+"&collectTime="+ct+"&collectorName="+cn+"&readoptType="+rt+"&cause="+cs;
		}
	});
	//删除单条
	$(".delSingle").click(function(){
		if(window.confirm("确定删除此任务吗？")){
			var tr = $(this).parent().parent();
			$('#action').val('del');
			$('#forwardURL').val('/page/result.jsp');
			$('#returnURL').val('javascript:history.back();');
			$('#id').val(tr.find("td").eq(1).text());
			document.form.submit();	
		}
	});
	//删除多条
	$("#delAll").click(function(){
		var len=$('[name=single]:checkbox').filter(':checked').length;
		if(len==0){
			alert("您没有选中行.");
		}else{
			if(window.confirm("确定删除所有选中的行吗？")){
				var str="";
				$('[name=single]:checkbox:checked').each(function(){
					str+=$(this).parent().parent().find('td').eq(1).text()+";";
				})
				$('#action').val('delMore');
				$('#forwardURL').val('/page/result.jsp');
				$('#returnURL').val('javascript:history.back();');
				$('#ids').val(str);
				document.form.submit();
			}
		}
	});
	//新增按钮，先查询出所有的任务，用于加载任务号
	$("#add").click(function(){
		$('#action').val('getAllTasks');
		$('#forwardURL').val('/page/rTaskAdd.jsp');
		document.form.submit();
	});
	 //全选
	    $("#selAll").click(function(){
		$('[name=single]:checkbox').attr("checked", this.checked );
	});
	 //单选
	$('[name=single]:checkbox').click(function(){
		var tmp=$('[name=single]:checkbox');
		$('#selAll').attr('checked',tmp.length==tmp.filter(':checked').length);
	});
	 //显示完整路径
	$('.showFilepath').click(function(){
		toggleScreen(1);
		$(this).siblings().filter('div').css('visibility','visible');
	});
	 //关闭已打开的路径
	$('div[class=showDiv]>div>a').click(function(){
		toggleScreen(0);
		$(this).parent().parent().css('visibility','hidden');
	});
	/*切换屏幕的显示状态*/
	function toggleScreen(v){
		window.scrollTo(0,0);
		var bo = document.getElementsByTagName('body')[0];
		var ht = document.getElementsByTagName('html')[0];
		bo.style.height='auto';
		bo.style.overflow='auto';
		ht.style.height='auto'; 
		if(v == 1){   
			bo.style.height='100%';
			bo.style.overflow='hidden';
			ht.style.height='100%';  
		}
	}
});