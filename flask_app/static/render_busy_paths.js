function update() {
    jQuery.get(base_url + '/get_data', (data) => {
        console.log("data", data);
        console.log("is empty not ",!jQuery.isEmptyObject(data));
        // $("#chartdiv").innerHtml = data;
        if(!jQuery.isEmptyObject(data)){
        $("#chartdiv").html(data);
        }
    });
}
