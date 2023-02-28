package uk.ac.gla.dcs.bigdata.providedstructures;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Represents a processed version of NewsArticle containing a HashMap with term frequencies and the document length, among other details.
 *
 */
public class PostNewsArticle implements Serializable {

	private static final long serialVersionUID = -8790870210613896654L;
	
	String id; // unique article identifier
	String title; // article title after tokenization, stopword removal and stemming
	int currentDocumentLength; // the number of terms within docmentTerms
	NewsArticle article;// Original news article object
	HashMap<String, Integer> documentTermCounts;// Hashmap of terms and the count of tem (bow)/ terms are : 
	//the title and the contents of the article body after tokenization, stopword removal and stemming

	
	public NewsArticle getArticle() {
		return article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}

	public PostNewsArticle() {}
	
	public PostNewsArticle(String id, String title, int currentDocumentLength,
			HashMap<String, Integer> documentTermCounts, NewsArticle article) {
		super();
		this.id = id;
		this.title = title;
		this.currentDocumentLength = currentDocumentLength;
		this.documentTermCounts = documentTermCounts;
		this.article = article;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String string) {
		this.title = string;
	}
	public int getCurrentDocumentLength() {
		return currentDocumentLength;
	}
	public void setCurrentDocumentLength(int currentDocumentLength) {
		this.currentDocumentLength = currentDocumentLength;
	}
	public HashMap<String, Integer> getDocumentTermCounts() {
		return documentTermCounts;
	}
	public void setDocumentTermCounts(HashMap<String, Integer> documentTermCounts) {
		this.documentTermCounts = documentTermCounts;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}



}
