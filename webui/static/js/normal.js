$(function(){
    resizeWindow();
})

$(window).resize(function(){
    resizeWindow();
});
 
function resizeWindow(){
    var window_height = $(window).height(),
        header_height = $("header").height(),
        footer_height = $("footer").height(),
        content_height = $(".content").height()

    var margin = (window_height-header_height-content_height-footer_height-180)/2;
    margin = Math.max(0, margin);
        
    $(".content").css('margin-top',margin+'px');
    $(".content").css('margin-bottom',margin+'px');
}