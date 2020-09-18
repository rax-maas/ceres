package me.itzg.tsdbcassandra.config;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;

/**
 * Provides a specific integer "collection" that can be bound to properties that provide
 * integer range lists converted with {@link StringToIntegerSetConverter}.
 */
public class IntegerSet implements Iterable<Integer> {

  final Set<Integer> content;

  IntegerSet(Set<Integer> content) {
    this.content = content;
  }

  public boolean isEmpty() {
    return content == null || content.isEmpty();
  }

  @Override
  public Iterator<Integer> iterator() {
    return content == null ? Collections.emptyIterator() : content.iterator();
  }

  @Override
  public Spliterator<Integer> spliterator() {
    return Spliterators.spliterator(iterator(), content.size(), 0);
  }

  public Stream<Integer> stream() {
    return content.stream();
  }
}
