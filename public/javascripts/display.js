/**
 * Created by deepanshu on 25/9/15.
 */

function getSearchResult() {

    //var input = $("#search-input").text();
    var input = document.getElementById("search-input").value
    console.log(input);
}


$(document).ready(function(){

    /*$("#click").click(function(){
        var result = document.getElementById("ex2").value
        console.log(result)
        var some = {param: result , name: "dummy"}
        $.ajax({
            method: "POST",
            url: "http://localhost:9000/getVertices",
            datatype: "json",
            contentType: "application/json; charset=utf-8",
            data: JSON.stringify(some),
            success : function (data) {
                console.log(data)
                for (i = 0; i < data.length; i++) {

                    var x = data[i]
                    var len = x.length
                    var y = x.substring(6, len - 2)
                    console.log(y)
                    $("#row").append("<tr><td>" + y + "</td></tr>")
                }
            }
            })
        })*/

    })