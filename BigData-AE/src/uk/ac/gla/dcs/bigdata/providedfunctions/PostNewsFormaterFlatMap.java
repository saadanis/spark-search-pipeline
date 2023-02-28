package uk.ac.gla.dcs.bigdata.providedfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.PostNewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

/**
 * Converts the NewsArticle into PostNewsFormater.
 *
 */
public class PostNewsFormaterFlatMap implements FlatMapFunction<NewsArticle,PostNewsArticle>   {

	private static final long serialVersionUID = -487456335610120046L;
	
	LongAccumulator totalDocsInCorpusAccumulator;
	LongAccumulator totalDocumentLengthInCorpusAccumulator;
	
	public PostNewsFormaterFlatMap(LongAccumulator totalDocsInCorpusAccumulator, LongAccumulator totalDocumentLengthInCorpusAccumulator) {
		this.totalDocsInCorpusAccumulator = totalDocsInCorpusAccumulator;
		this.totalDocumentLengthInCorpusAccumulator = totalDocumentLengthInCorpusAccumulator;
	}


	@Override
	public Iterator<PostNewsArticle> call(NewsArticle news) throws Exception {	
		
	    // handle null case
		if(news == null)
		    return new ArrayList<PostNewsArticle>(0).iterator();
	    // handle null title 
		if(news.getTitle()== null)
		    return new ArrayList<PostNewsArticle>(0).iterator();
	
		//Initialise needed objects for this function
		TextPreProcessor textPreproc = new TextPreProcessor();
		List<String> postNewsArtFullContent = new ArrayList<String>();
		
		// We want to select only 5 sub-contents of type "paragraph"
		// we loop on all sub-contents and filter out the ones with above requirements, and add to this counter
		int counter=0;
		
		// Loop around all sub-content and select first 5 of type "Paragraph"
		//performs tokenization, stop-word removal and stemming on the input text(content)
		if (news.getContents() != null) 
		for (ContentItem subcontent : news.getContents()) {
			// stop looking for sub-content after 5 sub-contents are found
			if(counter == 5)
				break;
			if(subcontent !=null)
			if(subcontent.getSubtype()!=null) {
				if(subcontent.getSubtype().toLowerCase().equals("paragraph")){
					postNewsArtFullContent.addAll(textPreproc.process(subcontent.getContent()));
					counter++;
				}
			}
		}
			
		
		//Initialise needed objects after everything is passed above
		HashMap<String, Integer> documentTermCounts = new HashMap<String, Integer>();
		List<PostNewsArticle> listPostNewsArtFullContent = new ArrayList<PostNewsArticle>(1);
		
		
		//performs tokenisation, stop-word removal and stemming on the input text(title)
		//add title to document terms
		postNewsArtFullContent.addAll(textPreproc.process(news.getTitle()));
		
		
		// get document length of all terms
		int sizeDocTerms=postNewsArtFullContent.size();
		
		// Add document length to accumulator.
		totalDocumentLengthInCorpusAccumulator.add(sizeDocTerms);
		
		// Add document count to accumulator.
		totalDocsInCorpusAccumulator.add(1);
		
		// The number of times each term appears in the documents basically like bag of words
		for (String term : postNewsArtFullContent) documentTermCounts.put(term, documentTermCounts.get(term)!= null? documentTermCounts.get(term)+1:1);

		// add all elements together
		listPostNewsArtFullContent.add(new PostNewsArticle(news.getId(), news.getTitle(), sizeDocTerms,
				documentTermCounts, new NewsArticle(news.getTitle())));
		
		//return
		return listPostNewsArtFullContent.iterator();
	}

	

	
	
}
