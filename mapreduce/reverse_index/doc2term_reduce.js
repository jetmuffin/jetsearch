/**
 * Created by jeff on 15/12/14.
 */
function doc2term_reduce(key, values){
	var result = {docs: []}
	for(var i = 0; i < values.length; i++){
		values[i].docs.forEach(function(doc){
			result.docs.push(doc)
		})
	}
	return result
}
