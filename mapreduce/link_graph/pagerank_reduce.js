/**
 * Created by jeff on 15/12/16.
 */
function pagerank_reduce(key, values){
    page = {
        _id: key,
        pr: 0,
        links: []
    }
    values.forEach(function(value){
       if(value.links.length){
           page.pr += value.pr * 0.15;
           page.links = value.links;
       } else {
           page.pr += value.pr * 0.85;
       }
    });
    return page
}