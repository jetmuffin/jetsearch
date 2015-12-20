/**
 * Created by jeff on 15/12/19.
 */
function ranking_map() {
    weight = {
        tf: 6,
        pr: 2,
        in_title: 1.5,
        in_links: 0.5
    }

    var id = this._id

    max_tf = 0
    max_pr = 0

    this.value.docs.forEach(function(doc) {
        if(doc.pr > max_pr) max_pr = doc.pr
        if(doc.tf > max_tf) max_tf = doc.tf
    })

    var value = {docs:[]}
    this.value.docs.forEach(function(doc) {
        var term_doc = {}
        term_doc.rating = doc.pr / max_pr * weight.pr +
                doc.tf / max_tf * weight.tf
        if(doc.in_title) term_doc.rating += weight.in_title
        if(doc.in_links) term_doc.rating += weight.in_links

        term_doc.pos = doc.pos
        term_doc.id = doc.page_id

        value.docs.push(term_doc)
    })

    value.docs.sort(function(a,b){return a.rating> b.rating?-1:1})
    emit(id, value)
}