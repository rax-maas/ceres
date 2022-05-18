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

package com.rackspace.ceres.app.services;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.springframework.stereotype.Service;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings("UnstableApiUsage") // due to guava
@Service
public class SeriesSetService {
  private static final Charset CHARSET = StandardCharsets.UTF_8;
  private final HashFunction hashFunction;
  private final Encoder base64Encoder;

  public SeriesSetService() {
    hashFunction = Hashing.murmur3_128();
    base64Encoder = Base64.getUrlEncoder();
  }

  public String hash(String metricName, Map<String, String> tags) {
    final Hasher hasher = hashFunction.newHasher()
        .putString(metricName, CHARSET);
    tags.entrySet().stream()
        .sorted(Entry.comparingByKey())
        .forEach(entry ->
            hasher.putString(entry.getKey(), CHARSET)
                .putString(entry.getValue(), CHARSET)
        );

    final HashCode hashCode = hasher.hash();

    // String encoding size of the murmur3 128-bit hashing is calculated as
    // (128 murmur hash bits) / (6 bits per base64 char) = 21.3; rounded up to the next integer = 22
    return base64Encoder.encodeToString(hashCode.asBytes())
        // base64 pads length to multiple of 3, such as
        // r4oa9hFoLqxF3eAXYrLb6g==
        // so two trailing padding characters are not useful for our string encoding needs
        .substring(0,22);
  }
}
