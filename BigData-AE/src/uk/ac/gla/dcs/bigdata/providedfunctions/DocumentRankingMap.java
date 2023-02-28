package uk.ac.gla.dcs.bigdata.providedfunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

/**
 * Removes the near-duplicate documents from the DocumentRanking.
 *
 */
public class DocumentRankingMap implements MapFunction<DocumentRanking, DocumentRanking> {

	private static final long serialVersionUID = 5098622524279690701L;

	@Override
	public DocumentRanking call(DocumentRanking docRanking) throws Exception {
		
		// create a list to hold the results
		List<RankedResult> filteredResults = new ArrayList<RankedResult>(0);
	
		// iterate through the list of ranked results
		outerloop:
		for(RankedResult result: docRanking.getResults()) {
			
			// Exit loops if ten filtered results are collected.
			if (filteredResults.size() == 10) {
				break outerloop;
			}
			
			// iterate through the list of filtered results
			for(RankedResult filteredResult: filteredResults) {
				
			// if the distance is <0.5, go to the next ranked result.
				if(TextDistanceCalculator.similarity(result.getArticle().getTitle(),
						filteredResult.getArticle().getTitle()) < 0.5)
					continue outerloop;
				
			}
			
			// otherwise, add the result to the filtered results.
			filteredResults.add(result);
		}
		
		return new DocumentRanking(docRanking.getQuery(), filteredResults);

	}	    
}

