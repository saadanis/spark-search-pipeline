package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.ArticleQueryScoreFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.ArticleQueryScoreToQuery;
import uk.ac.gla.dcs.bigdata.providedfunctions.DocumentRankingMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.FullDocumentRankingMapGroup;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterFlatMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.PostNewsFormaterFlatMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterFlatMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.TermFrequencyFlatMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.TotalFrequencyReduceGroups;
import uk.ac.gla.dcs.bigdata.providedfunctions.TupleToInteger;
import uk.ac.gla.dcs.bigdata.providedfunctions.TupleToString;
import uk.ac.gla.dcs.bigdata.providedstructures.ArticleQueryScore;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.PostNewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[4]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
//		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json"; // the entire dataset.
		
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
	}
	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Step 0: Load queries and articles.
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle + filter only sub types to only 5 sub types with "paragraph" 
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		
		// Step 1: Preprocess news articles to get the values.
		
		// Create accumulators to calculate total number of documents and the total length of the documents in the corpus.
		LongAccumulator totalDocsInCorpusAccumulator = spark.sparkContext().longAccumulator();
		LongAccumulator totalDocumentLengthInCorpusAccumulator = spark.sparkContext().longAccumulator();
				
		// We are going to focus on 3 properties news id, title, and content. Also by doing news pre-processing 
		// that will map news object to post processed news object that contain tokens of an article
		// and count of the number of terms within docmentTerms and The number of times each term appears in the document.
		Encoder<PostNewsArticle> postNewsArticleEncoder = Encoders.bean(PostNewsArticle.class);
		Dataset<PostNewsArticle> postNewsArticles =  news.flatMap(
				new PostNewsFormaterFlatMap(totalDocsInCorpusAccumulator, totalDocumentLengthInCorpusAccumulator),
				postNewsArticleEncoder);
		
		// Step 2: Get the term frequencies for all the query terms that exist within the corpus.
		
		// Get all the query terms.
		Dataset<String> queryTerms = queries.flatMap(new QueryFormaterFlatMap(), Encoders.STRING());
		List<String> queryTermsList = queryTerms.collectAsList();
		
		// Create a broadcast from the query terms list.
		Broadcast<List<String>> broadcastQueryTermsList = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryTermsList);
		
		// Get all the the term frequencies for the query terms using a flatmap.
		Encoder<Tuple2<String, Integer>> termFrequencyEncoder = Encoders.tuple(Encoders.STRING(), Encoders.INT());
		Dataset<Tuple2<String, Integer>> termFrequencies = postNewsArticles.flatMap(new TermFrequencyFlatMap(broadcastQueryTermsList), termFrequencyEncoder);
		
		// Group the term frequencies by the query term and map each query term to its frequency.
		TupleToString keyFunction = new TupleToString();
		TupleToInteger valueFunction = new TupleToInteger();
		KeyValueGroupedDataset<String, Integer> groupedTermFrequencies = termFrequencies.groupByKey(keyFunction, Encoders.STRING()).mapValues(valueFunction, Encoders.INT());
		
		// For each query term, reduce the groups to calculate the total term frequencies.
		Dataset<Tuple2<String, Integer>> termFrequencyIntegers = groupedTermFrequencies.reduceGroups(new TotalFrequencyReduceGroups());
		List<Tuple2<String, Integer>> totalTermFrequencyInCorpus = termFrequencyIntegers.collectAsList();
		
		// Get the values of and print the total number of documents in the corpus.
		long totalDocsInCorpus = totalDocsInCorpusAccumulator.value();
		System.out.println("totalDocsInCorpus: " + totalDocsInCorpus);

		// Get the values of and print the total length of all the documents in the corpus.
		long totalDocumentLengthInCorpus = totalDocumentLengthInCorpusAccumulator.value();
		System.out.println("totalDocumentLengthInCorpus: " + totalDocumentLengthInCorpus);
		
		// Calculate and print the average document length of the corpus.
		double averageDocumentLengthInCorpus = totalDocumentLengthInCorpus*1.0 / totalDocsInCorpus;
		System.out.println("averageDocumentLengthInCorpus: " + averageDocumentLengthInCorpus);
		
		// Step 3: Calculate the article query scores.
		
		// Collect the queries to send them as a broadcast.
		List<Query> queryList = queries.collectAsList();
		Broadcast<List<Query>> broadcastQueryList = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryList);
		
		// Create a broadcast of the total query term frequencies collected from Step 2.
		Broadcast<List<Tuple2<String, Integer>>> broadcastTotalTermFrequencyInCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalTermFrequencyInCorpus);
		
		// Calculate the scores for each query and document pair and return them as a dataset of ArticleQueryScore objects.
		Encoder<ArticleQueryScore> articleQueryScoreEncoder = Encoders.bean(ArticleQueryScore.class);
		Dataset<ArticleQueryScore> articleQueryScores = postNewsArticles.flatMap(new ArticleQueryScoreFormaterMap(
				broadcastQueryList,
				broadcastTotalTermFrequencyInCorpus,
				totalDocsInCorpus,
				averageDocumentLengthInCorpus
				), articleQueryScoreEncoder);
				
		// Step 4: Group and rank documents.
		
		// Group the ArticleQueryScore dataset by queries.
		ArticleQueryScoreToQuery keyFunction2 = new ArticleQueryScoreToQuery();
		KeyValueGroupedDataset<Query, ArticleQueryScore> groupedArticleQueryScores = articleQueryScores.groupByKey(keyFunction2, Encoders.bean(Query.class));
				
		// Rank the ArticleQueryScore groups and convert into the DocumentRanking object.
		Encoder<DocumentRanking> documentRankingEncoder = Encoders.bean(DocumentRanking.class);
		Dataset<DocumentRanking> fullDocumentRanking = groupedArticleQueryScores.mapGroups(new FullDocumentRankingMapGroup(), documentRankingEncoder);
		
		// Step 5: Remove redundancy.
		
		// Remove the redundancy from the DocumentRanking dataset using similarity scores.
		Dataset<DocumentRanking> documentRanking = fullDocumentRanking.map(new DocumentRankingMap(), documentRankingEncoder);
		List<DocumentRanking> documentRankingList = documentRanking.collectAsList();
		
		// Step 6: Retrieve the NewsArticle for each document and re-add it into the final DocumentRanking list.
		
		// Get all the document ids in the final document rankings.
		List<String> docids = new ArrayList<String>();
		for(DocumentRanking docRank: documentRankingList) {
			for (RankedResult rankedResult: docRank.getResults()) {
				docids.add(rankedResult.getDocid());
			}
		}
		
		// Create a broadcast for the document ids.
		Broadcast<List<String>> broadcastDocids = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(docids);
		
		// Map through the the news articles and get only the articles that are in the final document ids.
		Dataset<NewsArticle> rankedNewsArticles = news.flatMap(new NewsFormaterFlatMap(broadcastDocids), Encoders.bean(NewsArticle.class));
		List<NewsArticle> rankedNewsArticlesList = rankedNewsArticles.collectAsList();
		
		// Set the collected articles back into the final DocumentRanking list.
		for(DocumentRanking docRank: documentRankingList) {
			for (RankedResult rankedResult: docRank.getResults()) {
				for (NewsArticle newsArticle: rankedNewsArticlesList) {
					if (rankedResult.getDocid().equals(newsArticle.getId())) {
						rankedResult.setArticle(newsArticle);
					}
				}
			}
		}
		
		// Print the results.
		for(DocumentRanking docRank: documentRankingList) {
			System.out.println(docRank.getQuery().getOriginalQuery());
			for (RankedResult result: docRank.getResults()) {
				System.out.printf("%.2f: %s\n", result.getScore(), result.getArticle().getTitle());
			}
			System.out.println("\n***\n");
		}
		
		return documentRankingList; // replace this with the the list of DocumentRanking output by your topology
	}
	
}
