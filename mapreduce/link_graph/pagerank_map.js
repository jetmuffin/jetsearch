/**
 * Created by jeff on 15/12/16.
 */
function pagerank_map(){
    var page = this;
    if(page.value.links.length > 0){
        this.value.links.forEach(function(link){
            emit(link, {
                pr: page.value.pr / page.value.links.length,
                links: [],
                length: page.value.length
            })
        });
    }
    emit(page._id, {
        pr: 1 / page.value.length,
        links: page.value.links,
        length: page.value.length
    })
}