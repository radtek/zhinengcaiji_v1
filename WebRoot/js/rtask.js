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
	//��ѯ��ť�����ֲ�ѯ
	$("#query").click(function(){
		subQuery(1);
	});
	//���ð�ť
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
	//��ҳ
	$('#firstPage').click(function(){
		var currPage = Number($('#firPageLabel').text());
		if(currPage<=1){
			alert("�Ѿ�����ҳ.");
			return;
		}
		subQuery(1);
	});
	//��һҳ
	$('#frontPage').click(function(){
		var currPage = Number($('#firPageLabel').text());
		if(currPage<=1){
			alert("�Ѿ�����ҳ�ˣ�");
			return;
		}
		subQuery(currPage-1);
	});
	//��һҳ
	$('#nextPage').click(function(){
		var currPage = Number($('#firPageLabel').text());
		var totalPage = Number($('#totalPageLabel').text());
		if(currPage==totalPage){
			alert("�Ѿ���ĩҳ�ˣ�");
			return;
		}
		subQuery(currPage+1);
	});
	//ĩҳ
	$('#lastPage').click(function(){
		var currPage = Number($('#firPageLabel').text());
		var totalPage = Number($('#totalPageLabel').text());
		if(currPage==totalPage){
			alert("�Ѿ���ĩҳ�ˣ�");
			return;
		}
		subQuery(totalPage);
	});
	//��ǰҳ
	$('#currentPage').click(function(){
		var obj = $('#pageNum');
		var currentNum = obj.val();
		var currPage = Number($('#firPageLabel').text());
		var totalPage = Number($('#totalPageLabel').text());
		if(isNaN(currentNum)||currentNum==""){
			alert("����������");
			obj.focus();
			return;
		}else if(currentNum == currPage){
			alert("�Ѿ��ڴ�ҳ�ˣ�");
			obj.focus();
			return;
		}else if(currentNum<1||currentNum>totalPage){
			alert("����ֻ����1-"+totalPage+"֮��.");
			obj.focus();
			return;
		}
		subQuery(currentNum);
	});
	/*requestPage Ҫ�����ҳ��*/
	function subQuery(requestPage){
		$('#action').val('query');
		$('#forwardURL').val('/page/rTask.jsp');	
		$('#returnURL').val('javascript:history.back();');
		$('#page').val(requestPage);
		document.form.submit();
	}
	
	/*sql����ѯ*/
	$("#sqlSelect").change(function(){
		$("#sqlCondition").val($(this).val());
	});
	//��������
	$("#local").click(function(){
		//$("#reset").triggerHandler("click");
		resetVal();
		$("#collector_name").val($("#hostName").val());
		subQuery(1);
	});
	//��������
	$("#newRtask").click(function(){
		resetVal();
		$("#collectStatus").val("0");
		subQuery(1);
	});
	//��������
	$("#hisRtask").click(function(){
		resetVal();
		$("#collectStatus").val("3");
		subQuery(1);
	});
	//ʧЧ����
	$("#invaliRtask").click(function(){
		resetVal();
		$("#collectStatus").val("-1");
		subQuery(1);
	});
	//���°�ť
	$(".uptSingle").click(function(){
		var tr = $(this).parent().parent();
		$('#action').val('get');
		$('#forwardURL').val('/page/rTaskUpdate.jsp');
		$('#returnURL').val('javascript:history.back();');
		$('#id').val(tr.find("td").eq(1).text());
		document.form.submit();
	});
	//���ť
	$(".actSingle").click(function(){
		if(window.confirm("ȷ�������������")){
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
	//ɾ������
	$(".delSingle").click(function(){
		if(window.confirm("ȷ��ɾ����������")){
			var tr = $(this).parent().parent();
			$('#action').val('del');
			$('#forwardURL').val('/page/result.jsp');
			$('#returnURL').val('javascript:history.back();');
			$('#id').val(tr.find("td").eq(1).text());
			document.form.submit();	
		}
	});
	//ɾ������
	$("#delAll").click(function(){
		var len=$('[name=single]:checkbox').filter(':checked').length;
		if(len==0){
			alert("��û��ѡ����.");
		}else{
			if(window.confirm("ȷ��ɾ������ѡ�е�����")){
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
	//������ť���Ȳ�ѯ�����е��������ڼ��������
	$("#add").click(function(){
		$('#action').val('getAllTasks');
		$('#forwardURL').val('/page/rTaskAdd.jsp');
		document.form.submit();
	});
	 //ȫѡ
	    $("#selAll").click(function(){
		$('[name=single]:checkbox').attr("checked", this.checked );
	});
	 //��ѡ
	$('[name=single]:checkbox').click(function(){
		var tmp=$('[name=single]:checkbox');
		$('#selAll').attr('checked',tmp.length==tmp.filter(':checked').length);
	});
	 //��ʾ����·��
	$('.showFilepath').click(function(){
		toggleScreen(1);
		$(this).siblings().filter('div').css('visibility','visible');
	});
	 //�ر��Ѵ򿪵�·��
	$('div[class=showDiv]>div>a').click(function(){
		toggleScreen(0);
		$(this).parent().parent().css('visibility','hidden');
	});
	/*�л���Ļ����ʾ״̬*/
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