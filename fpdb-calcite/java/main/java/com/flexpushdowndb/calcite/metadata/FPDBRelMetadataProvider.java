package com.flexpushdowndb.calcite.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.*;

public class FPDBRelMetadataProvider extends ChainedRelMetadataProvider {
  public static final FPDBRelMetadataProvider INSTANCE = new FPDBRelMetadataProvider();

  /**
   * customized handlers are:
   *  - FPDBRelMdColumnOrigins
   *  - FPDBRelMdRowCount
   */
  private FPDBRelMetadataProvider() {
    super(
        ImmutableList.of(
                RelMdPercentageOriginalRows.SOURCE,
                FPDBRelMdColumnOrigins.SOURCE,
                RelMdExpressionLineage.SOURCE,
                RelMdTableReferences.SOURCE,
                RelMdNodeTypes.SOURCE,
                FPDBRelMdRowCount.SOURCE,
                RelMdMaxRowCount.SOURCE,
                RelMdMinRowCount.SOURCE,
                RelMdUniqueKeys.SOURCE,
                RelMdColumnUniqueness.SOURCE,
                RelMdPopulationSize.SOURCE,
                RelMdSize.SOURCE,
                RelMdParallelism.SOURCE,
                RelMdDistribution.SOURCE,
                RelMdLowerBoundCost.SOURCE,
                RelMdMemory.SOURCE,
                RelMdDistinctRowCount.SOURCE,
                RelMdSelectivity.SOURCE,
                RelMdExplainVisibility.SOURCE,
                RelMdPredicates.SOURCE,
                RelMdAllPredicates.SOURCE,
                RelMdCollation.SOURCE));
  }
}
