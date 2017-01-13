package com.mycompany.app;

import java.io.Serializable;
import java.util.Comparator;

/*
 * When giving a Comparator<T> to Spark (for example in the max function), one
 * gets an exception: org.apache.spark.SparkException: Task not serializable
 * The following interface allows to get Serializable comparators.
 * It is probably not the most elegant solution but does the trick for
 * this small project.
 */
public interface SerializableComparator<T> extends Comparator<T>, Serializable
{
	static <T> SerializableComparator<T> serialize(SerializableComparator<T> comparator)
	{
		return comparator;
	}
}