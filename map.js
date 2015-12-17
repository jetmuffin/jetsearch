function map() {
	var page_id = this.page_id;
    this.terms.forEach(function(term){
    	var doc = {"doc_id":page_id, "tf":term.tf, "in_title": term.in_title, "in_links": term.in_links}
    	emit(term.word, doc);
    });
};