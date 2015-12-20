/**
 * Created by jeff on 15/12/16.
 */
function pagerank_reduce(key, values){
    page = {
        _id: key,
        pr: 0,
        length: 0,
        links: []
    }
    values.forEach(function(value){
       if(value.links.length){
           page.pr += value.pr * 0.2;
           page.links = value.links;
           page.length = value.length
       } else {
           page.pr += value.pr * 0.8;
       }
    });
    return page
}