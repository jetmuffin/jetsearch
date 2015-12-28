/**
 * Created by jeff on 15/12/14.
 */
function doc2term_map() {
	var page = this
    this.terms.forEach(function(term){
    	var doc = {
			page_id: page._id,
			tf: term.tf,
			title_tf: term.title_tf,
			pos: term.pos,
			pr: page.pr
		};
		var value = {
			docs: [doc]
		};
    	emit(term.word, value);
    });
}