package uk.ac.gla.dcs.bigdata.providedfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.ArticleQueryScore;
import uk.ac.gla.dcs.bigdata.providedstructures.PostNewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;

/**
 * Calculates the DPH for each query for an article and returns as an iterator of ArticleQueryScore.
 *
 */
public class ArticleQueryScoreFormaterMap implements FlatMapFunction<PostNewsArticle,ArticleQueryScore>   {

	
	private static final long serialVersionUID = 7657428036496798076L;
	
	long totalDocsInCorpus;
	double averageDocumentLengthInCorpus;
	
	Broadcast<List<Query>> broadcastQueries;
	Broadcast<List<Tuple2<String, Integer>>> broadcastTotalTermFrequencyInCorpus;
	
	public ArticleQueryScoreFormaterMap(Broadcast<List<Query>> broadcastQueries, Broadcast<List<Tuple2<String, Integer>>> broadcastTotalTermFrequencyInCorpus, long totalDocsInCorpus, double averageDocumentLengthInCorpus) {
		this.broadcastQueries = broadcastQueries;
		this.broadcastTotalTermFrequencyInCorpus = broadcastTotalTermFrequencyInCorpus;
		this.totalDocsInCorpus = totalDocsInCorpus;
		this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
	}

	@Override
	public Iterator<ArticleQueryScore> call(PostNewsArticle postNewsArticle) throws Exception {
		
		// Get the broadcasted queries and total term frequencies.
		List<Query> queries = broadcastQueries.value();
		List<Tuple2<String, Integer>> totalTermFrequenciesInCorpus = broadcastTotalTermFrequencyInCorpus.value();
		
		// Create an empty list of article query scores.
		List<ArticleQueryScore> articleQueryScores = new ArrayList<ArticleQueryScore>(queries.size());
		
		// Iterate through each query.
		for (Query query: queries) {
			
			// Initialize the total DPH score to 0.
			double totalDPHScore = 0;
			
			// Iterate through each query term for every query.
			for (String queryTerm: query.getQueryTerms()) {
				
				// Get the term frequency for the query term in the article.
				int termFrequencyInCurrentDocument = 0;
				if (postNewsArticle.getDocumentTermCounts().get(queryTerm) != null) {
					termFrequencyInCurrentDocument = postNewsArticle.getDocumentTermCounts().get(queryTerm);
				}
				
				// Get the total term frequency for the query term in the corpus from the broadcast.
				int totalTermFrequencyInCorpus = 0;
				for (Tuple2<String, Integer> ttf: totalTermFrequenciesInCorpus) {
					if (ttf._1.equals(queryTerm)) {
						totalTermFrequencyInCorpus = ttf._2;
						break;
					}
				}
				
				// Calculate the DPH score for the current query term.
				double currentTermDPHScore = DPHScorer.getDPHScore(
						(short)termFrequencyInCurrentDocument,
						totalTermFrequencyInCorpus,
						postNewsArticle.getCurrentDocumentLength(),
						averageDocumentLengthInCorpus,
						totalDocsInCorpus);
				
				// If the current DPH score is NaN, set it to 0.
				if (Double.isNaN(currentTermDPHScore)) currentTermDPHScore = 0;
				
				// Sum the DPH scores.
				totalDPHScore += currentTermDPHScore;
			}
			
			// If the DPH score is greater than 0, add it to the articleQueryScores list.
			if (totalDPHScore > 0)
				articleQueryScores.add(new ArticleQueryScore(postNewsArticle.getId(), query, postNewsArticle.getArticle(), totalDPHScore));
		}
		
		// Return the articleQueryScores list with all the article query scores.
		return articleQueryScores.iterator();
	}
}
