package uk.ac.gla.dcs.bigdata.providedfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.MapGroupsFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ArticleQueryScore;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

/**
 * Converts the ArticleQueryScores for a Query to RankedResults, reorders them and returns the DocumentRanking.
 *
 */
public class FullDocumentRankingMapGroup implements MapGroupsFunction<Query, ArticleQueryScore, DocumentRanking> {

	private static final long serialVersionUID = -4168175988891464530L;

	@Override
	public DocumentRanking call(Query key, Iterator<ArticleQueryScore> values) throws Exception {
		
		// Create an empty DocumentRanking object and an empty list of RankedResults.
		DocumentRanking documentRanking = new DocumentRanking();
		List<RankedResult> results = new ArrayList<RankedResult>();
		
		// Set the documentRanking query.
		documentRanking.setQuery(key);
		
		// For every value in the iterator of ArticleQueryScore, it creates a new RankedResult object and adds to the results list.
		while (values.hasNext()) {
			ArticleQueryScore articleQueryScore = values.next();
			results.add(new RankedResult(articleQueryScore.getDocid(),articleQueryScore.getArticle(), articleQueryScore.getScore()));
		}
		
		// The list is sorted and reversed to be in descending order of scores.
		Collections.sort(results);
		Collections.reverse(results);
		
		// The list is added to the documentRanking.
		documentRanking.setResults(results);
		
		// Return documentRanking.
		return documentRanking;
	}

}
