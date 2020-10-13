/*
 * Copyright 2020 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspace.ceres.app.config;

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
