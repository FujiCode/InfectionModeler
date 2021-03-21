function load_column_headers(df_columns_json, page) {
  var i = 0;	
  var str1='<option value=""></option>';
  var obj1 = JSON.parse(df_columns_json.replace(/&#34;/g, '"'));
  for(i = 0; i < obj1.column_names.length; i++) {
	str1 = str1 +
	'<option value="' + obj1.column_names[i].col + '">' + obj1.column_names[i].col + '</option>'
  }
  if(page == 'data') {
      document.getElementById("columns_sort").innerHTML = str1;
      document.getElementById("index_column").innerHTML = str1;
      document.getElementById("columns_drop").innerHTML = str1;
      document.getElementById("null_drop").innerHTML = str1;
      document.getElementById("average_duplicates1").innerHTML = str1;
      document.getElementById("average_duplicates2").innerHTML = str1;
      document.getElementById("target").innerHTML = str1;
  } else if (page == 'model') {
	  document.getElementById("select_features").innerHTML = str1;
    }
}
function openOptions(open) {
	var x1 = document.getElementById("options1");
	if (open == "False") {
		x1.style.display = "none"
	} else if (open == "True") {
		x1.style.display = "block"
	}
}