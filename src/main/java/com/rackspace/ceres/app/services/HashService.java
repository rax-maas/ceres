package com.rackspace.ceres.app.services;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@Service
public class HashService {
    private final HashFunction hashFunction;
    private static final Charset HASHING_CHARSET = StandardCharsets.UTF_8;

    @Autowired
    public HashService() {
        hashFunction = Hashing.murmur3_32();
    }

    public HashCode getHashCode(String string1, String string2) {
        return hashFunction.newHasher()
                .putString(string1, HASHING_CHARSET)
                .putString(string2, HASHING_CHARSET)
                .hash();
    }

    public int getPartition(HashCode hashCode, int partitions) {
        return Hashing.consistentHash(hashCode, partitions);
    }
}
