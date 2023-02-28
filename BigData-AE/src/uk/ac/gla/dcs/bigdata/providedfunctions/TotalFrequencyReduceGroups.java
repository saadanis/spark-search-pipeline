package uk.ac.gla.dcs.bigdata.providedfunctions;

import org.apache.spark.api.java.function.ReduceFunction;

/**
 * Reduces the scores by summing.
 *
 */
public class TotalFrequencyReduceGroups implements ReduceFunction<Integer> {

	private static final long serialVersionUID = -6203332876005840039L;

	@Override
	public Integer call(Integer v1, Integer v2) throws Exception {
		
		// Sum the scores.
		return v1+v2;
	}

}
