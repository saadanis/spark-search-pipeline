package uk.ac.gla.dcs.bigdata.providedfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ArticleQueryScore;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

/**
 * Returns the query from the ArticleQueryScore. 
 *
 */
public class ArticleQueryScoreToQuery implements MapFunction<ArticleQueryScore, Query> {

	private static final long serialVersionUID = 3461040687521268325L;

	@Override
	public Query call(ArticleQueryScore articleQueryScore) throws Exception {
		
		// Return the query from the ArticleQueryScore object.
		return articleQueryScore.getQuery();
	}

}
