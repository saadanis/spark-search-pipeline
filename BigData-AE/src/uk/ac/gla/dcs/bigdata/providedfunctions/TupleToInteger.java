package uk.ac.gla.dcs.bigdata.providedfunctions;

import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple2;

/**
 * Returns the integer value from the Tuple.
 *
 */
public class TupleToInteger implements MapFunction<Tuple2<String, Integer>, Integer> {

	private static final long serialVersionUID = -3739972634810074547L;

	@Override
	public Integer call(Tuple2<String, Integer> value) throws Exception {
		// TODO Auto-generated method stub
		return value._2;
	}

}
