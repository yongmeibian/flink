/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Period;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parsing utilities for {@link Expressions#interval(String, DataTypes.Resolution, DataTypes.Resolution)}.
 */
final class IntervalParsingUtils {
	/**
	 * Creates a parser for a given INTERVAL dataType.
	 *
	 * <p>It supports only types with roots: {@link LogicalTypeRoot#INTERVAL_YEAR_MONTH} and
	 * {@link LogicalTypeRoot#INTERVAL_DAY_TIME} and only of a combined resolution such as {@code YEAR TO MONTH}
	 * or {@code DAY TO MINUTE}.
	 */
	static Function<String, Object> intervalParser(DataType dataType) {
		if (LogicalTypeChecks.hasRoot(dataType.getLogicalType(), LogicalTypeRoot.INTERVAL_YEAR_MONTH)) {
			YearMonthIntervalType yearMonthIntervalType = (YearMonthIntervalType) dataType.getLogicalType();
			if (yearMonthIntervalType.getResolution() == YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH) {
				return conversionYearToMonth(dataType, yearMonthIntervalType);
			} else {
				throw new UnsupportedOperationException(
					"Unsupported resolution " + yearMonthIntervalType.getResolution());
			}
		} else if (LogicalTypeChecks.hasRoot(dataType.getLogicalType(), LogicalTypeRoot.INTERVAL_DAY_TIME)) {
			DayTimeIntervalType dayTimeIntervalType = (DayTimeIntervalType) dataType.getLogicalType();
			switch (dayTimeIntervalType.getResolution()){
				case DAY_TO_HOUR:
					return conversionDayToHour(dataType, dayTimeIntervalType);
				case DAY_TO_MINUTE:
					return conversionDayToMinute(dataType, dayTimeIntervalType);
				case DAY_TO_SECOND:
					return conversionDayToSecond(dataType, dayTimeIntervalType);
				case HOUR_TO_MINUTE:
					return conversionHourToMinute(dataType);
				case HOUR_TO_SECOND:
					return conversionHourToSecond(dataType, dayTimeIntervalType);
				case MINUTE_TO_SECOND:
					return conversionMinuteToSecond(dataType, dayTimeIntervalType);
				default:
					throw new UnsupportedOperationException(
						"Unsupported resolution " + dayTimeIntervalType.getResolution());
			}
		} else {
			throw new IllegalArgumentException("Only interval types are supported.");
		}
	}

	private static Function<String, Object> conversionYearToMonth(
		DataType dataType,
		YearMonthIntervalType yearMonthIntervalType) {
		String intervalPattern = String.format(
			"(\\d{1,%d})-(\\d{1,2})",
			yearMonthIntervalType.getYearPrecision());
		return value -> {
			Matcher m = Pattern.compile(intervalPattern).matcher(value);
			if (m.matches()) {
				int year = Integer.parseInt(m.group(1));
				int month = Integer.parseInt(m.group(2));
				return Period.of(year, month, 0);
			} else {
				throw new ValidationException("Incorrect format for " + dataType);
			}
		};
	}

	private static Function<String, Object> conversionDayToHour(
		DataType dataType,
		DayTimeIntervalType dayTimeIntervalType) {
		String intervalPattern = String.format(
			"(\\d{1,%d}) (\\d{1,2})",
			dayTimeIntervalType.getDayPrecision());
		return value -> {
			Matcher m = Pattern.compile(intervalPattern).matcher(value);
			if (m.matches()) {
				long days = Long.parseLong(m.group(1));
				long hours = Long.parseLong(m.group(2));
				return Duration.ofDays(days).plusHours(hours);
			} else {
				throw new ValidationException(String.format(
					"Incorrect format for %s. Expected: %s",
					dataType,
					intervalPattern));
			}
		};
	}

	private static Function<String, Object> conversionDayToMinute(
		DataType dataType,
		DayTimeIntervalType dayTimeIntervalType) {
		String intervalPattern = String.format(
			"(\\d{1,%d}) (\\d{1,2}):(\\d{1,2})",
			dayTimeIntervalType.getDayPrecision());
		return value -> {
			Matcher m = Pattern.compile(intervalPattern).matcher(value);
			if (m.matches()) {
				long days = Long.parseLong(m.group(1));
				long hours = Long.parseLong(m.group(2));
				long minutes = Long.parseLong(m.group(3));
				return Duration.ofDays(days).plusHours(hours).plusMinutes(minutes);
			} else {
				throw new ValidationException(String.format(
					"Incorrect format for %s. Expected: %s",
					dataType,
					intervalPattern));
			}
		};
	}

	private static Function<String, Object> conversionDayToSecond(
		DataType dataType,
		DayTimeIntervalType dayTimeIntervalType) {
		String intervalPattern = String.format(
			"(\\d{1,%d}) (\\d{1,2}):(\\d{1,2}):(\\d{1,2})\\.?(\\d{0,%d})",
			dayTimeIntervalType.getDayPrecision(),
			dayTimeIntervalType.getFractionalPrecision()
		);
		return value -> {
			Matcher m = Pattern.compile(intervalPattern).matcher(value);
			if (m.matches()) {
				long days = Long.parseLong(m.group(1));
				long hours = Long.parseLong(m.group(2));
				long minutes = Long.parseLong(m.group(3));
				long seconds = Long.parseLong(m.group(4));
				long nanos = parseNanos(m.group(5));
				return Duration.ofDays(days)
					.plusHours(hours)
					.plusMinutes(minutes)
					.plusSeconds(seconds)
					.plusNanos(nanos);
			} else {
				throw new ValidationException(String.format(
					"Incorrect format for %s. Expected: %s",
					dataType,
					intervalPattern));
			}
		};
	}

	private static Function<String, Object> conversionHourToMinute(DataType dataType) {
		String intervalPattern = "(\\d{1,2}):(\\d{1,2})";
		return value -> {
			Matcher m = Pattern.compile(intervalPattern).matcher(value);
			if (m.matches()) {
				long hours = Long.parseLong(m.group(2));
				long minutes = Long.parseLong(m.group(3));
				return Duration.ofHours(hours)
					.plusMinutes(minutes);
			} else {
				throw new ValidationException(String.format(
					"Incorrect format for %s. Expected: %s",
					dataType,
					intervalPattern));
			}
		};
	}

	private static Function<String, Object> conversionHourToSecond(
		DataType dataType,
		DayTimeIntervalType dayTimeIntervalType) {
		String intervalPattern = String.format(
			"(\\d{1,2}):(\\d{1,2}):(\\d{1,2})\\.?(\\d{0,%d})",
			dayTimeIntervalType.getFractionalPrecision()
		);
		return value -> {
			Matcher m = Pattern.compile(intervalPattern).matcher(value);
			if (m.matches()) {
				long hours = Long.parseLong(m.group(2));
				long minutes = Long.parseLong(m.group(3));
				long seconds = Long.parseLong(m.group(4));
				long nanos = parseNanos(m.group(5));
				return Duration.ofHours(hours)
					.plusMinutes(minutes)
					.plusSeconds(seconds)
					.plusNanos(nanos);
			} else {
				throw new ValidationException(String.format(
					"Incorrect format for %s. Expected: %s",
					dataType,
					intervalPattern));
			}
		};
	}

	private static Function<String, Object> conversionMinuteToSecond(
		DataType dataType,
		DayTimeIntervalType dayTimeIntervalType) {
		String intervalPattern = String.format(
			"(\\d{1,2}):(\\d{1,2})\\.?(\\d{0,%d})",
			dayTimeIntervalType.getFractionalPrecision()
		);
		return value -> {
			Matcher m = Pattern.compile(intervalPattern).matcher(value);
			if (m.matches()) {
				long minutes = Long.parseLong(m.group(3));
				long seconds = Long.parseLong(m.group(4));
				long nanos = parseNanos(m.group(5));
				return Duration.ofMinutes(minutes)
					.plusSeconds(seconds)
					.plusNanos(nanos);
			} else {
				throw new ValidationException(String.format(
					"Incorrect format for %s. Expected: %s",
					dataType,
					intervalPattern));
			}
		};
	}

	private static long parseNanos(String fractionalSeconds) {
		return new BigDecimal("0." + fractionalSeconds)
			.multiply(BigDecimal.valueOf(1_000_000_000))
			.longValue();
	}
}
