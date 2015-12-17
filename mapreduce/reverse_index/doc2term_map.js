/**
 * Created by jeff on 15/12/14.
 */
function doc2term_map() {
	var page_id = this.page_id;
    this.terms.forEach(function(term){
    	var doc = {
			doc_id: page_id,
			tf: term.tf,
			in_title: term.in_title,
			in_links: term.in_links
		};
		var value = {
			docs: [doc]
		};
    	emit(term.word, value);
    });
};