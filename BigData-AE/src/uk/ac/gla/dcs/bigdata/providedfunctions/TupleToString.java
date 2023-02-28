package uk.ac.gla.dcs.bigdata.providedfunctions;

import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple2;

/**
 * Returns the String value from from the Tuple.
 *
 */
public class TupleToString implements MapFunction<Tuple2<String, Integer>, String> {

	private static final long serialVersionUID = -2886613011918770089L;

	@Override
	public String call(Tuple2<String, Integer> tuple) throws Exception {
		return tuple._1;
	}
}
