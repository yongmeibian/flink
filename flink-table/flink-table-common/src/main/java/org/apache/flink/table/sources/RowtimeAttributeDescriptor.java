package org.apache.flink.table.sources;

import org.apache.flink.table.sources.tsextractors.TimestampExtractor;
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;

import java.util.Objects;

/**
 * Describes a rowtime attribute of a {@link TableSource}.
 */
public final class RowtimeAttributeDescriptor {

	private final String attributeName;
	private final TimestampExtractor timestampExtractor;
	private final WatermarkStrategy watermarkStrategy;

	public RowtimeAttributeDescriptor(
			String attributeName,
			TimestampExtractor timestampExtractor,
			WatermarkStrategy watermarkStrategy) {
		this.attributeName = attributeName;
		this.timestampExtractor = timestampExtractor;
		this.watermarkStrategy = watermarkStrategy;
	}

	/** Returns the name of the rowtime attribute. */
	public String getAttributeName() {
		return attributeName;
	}

	/** Returns the [[TimestampExtractor]] for the attribute. */
	public TimestampExtractor getTimestampExtractor() {
		return timestampExtractor;
	}

	/** Returns the [[WatermarkStrategy]] for the attribute. */
	public WatermarkStrategy getWatermarkStrategy() {
		return watermarkStrategy;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		RowtimeAttributeDescriptor that = (RowtimeAttributeDescriptor) o;
		return Objects.equals(attributeName, that.attributeName) &&
			Objects.equals(timestampExtractor, that.timestampExtractor) &&
			Objects.equals(watermarkStrategy, that.watermarkStrategy);
	}

	@Override
	public int hashCode() {
		return Objects.hash(attributeName, timestampExtractor, watermarkStrategy);
	}
}
