package uk.ac.gla.dcs.bigdata.providedfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.PostNewsArticle;

/**
 * Converts the PostNewsArticle to a list of tuples containing the term and frequency of every term that occurs in any query.
 *
 */
public class TermFrequencyFlatMap implements FlatMapFunction<PostNewsArticle, Tuple2<String, Integer>>{
	
	private static final long serialVersionUID = -5386865671058500833L;
	
	Broadcast<List<String>> broadcastQueryTermsList;

	public TermFrequencyFlatMap(Broadcast<List<String>> broadcastQueryTermsList) {
		this.broadcastQueryTermsList = broadcastQueryTermsList;
	}

	@Override
	public Iterator<Tuple2<String, Integer>> call(PostNewsArticle article) throws Exception {
		
		List<Tuple2<String,Integer>> termFrequencies = new ArrayList<Tuple2<String, Integer>>();
		List<String> queryTermsList = broadcastQueryTermsList.value();
		
		// For every term in the article, if the term exists in the list of query terms, add the term and its count to the list.
		for(Entry<String, Integer> term: article.getDocumentTermCounts().entrySet()) {
			if (queryTermsList.contains(term.getKey())) {
				termFrequencies.add(new Tuple2<String,Integer>(term.getKey(), term.getValue()));
			}
		}
		
		return termFrequencies.iterator();
	}
}
