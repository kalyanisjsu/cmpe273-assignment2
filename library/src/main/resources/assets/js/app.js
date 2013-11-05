 $(":button").click(function(){
	 var isbn = this.id;
	 $(":button").attr("disabled", true)
	 var request =  $.ajax({
		 	type: 'put',
		    url: "http://localhost:8001/library/v1/books/"+ isbn +"?status=lost",
		    contentType:'application/json'
		    });
	 		request.success(function(data, textStatus, jqXHR) {
		    alert("Status Updated Successfully");
		    });
	 		request.error(function(jqXHR, textStatus, errorThrown) {
			alert("Failed")});
	 		$(":button").attr("disabled", true)
	 		
 });