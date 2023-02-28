package uk.ac.gla.dcs.bigdata.providedfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/**
 * Return a NewsArticle if its id is included in the boradcastsed Document IDs list.
 *
 */
public class NewsFormaterFlatMap implements FlatMapFunction<NewsArticle, NewsArticle> {

	private static final long serialVersionUID = 7825981991579183745L;

	Broadcast<List<String>> broadcastDocids;
	
	public NewsFormaterFlatMap(Broadcast<List<String>> broadcastDocids) {
		this.broadcastDocids = broadcastDocids;
	}
	
	@Override
	public Iterator<NewsArticle> call(NewsArticle newsArticle) throws Exception {
		
		// Get the docid values from the broadcast.
		List<String> docids = broadcastDocids.value();
		
		// Create an empty NewsArticle list.
		List<NewsArticle> newsArticles = new ArrayList<NewsArticle>();
		
		// If the docids contain the id of the newsArticle, add it to the list.
		if (docids.contains(newsArticle.getId()))
			newsArticles.add(newsArticle);

		// Return the list as an iterator.
		return newsArticles.iterator();
	}

}
