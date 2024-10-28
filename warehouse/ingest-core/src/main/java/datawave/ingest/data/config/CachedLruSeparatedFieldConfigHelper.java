package datawave.ingest.data.config;

import java.util.Map;
import java.util.function.Predicate;

import org.apache.commons.collections4.map.LRUMap;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.google.common.annotations.VisibleForTesting;

public class CachedLruSeparatedFieldConfigHelper implements FieldConfigHelper {
    private final FieldConfigHelper underlyingHelper;
    private final Map<String,Boolean> indexedCache;
    private final Map<String,Boolean> reverseIndexCache;
    private final Map<String,Boolean> tokenizedCache;
    private final Map<String,Boolean> reverseTokenizedCache;
    private final Map<String,Boolean> storedCache;
    private final Map<String,Boolean> indexedOnlyCache;

    public CachedLruSeparatedFieldConfigHelper(FieldConfigHelper helper, int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("Limit must be a positive integer");
        }
        this.underlyingHelper = helper;
        this.indexedCache = new LRUMap<>(limit);
        this.reverseIndexCache = new LRUMap<>(limit);
        this.tokenizedCache = new LRUMap<>(limit);
        this.reverseTokenizedCache = new LRUMap<>(limit);
        this.storedCache = new LRUMap<>(limit);
        this.indexedOnlyCache = new LRUMap<>(limit);
    }

    @Override
    public boolean isStoredField(String fieldName) {
        return getFieldResult(storedCache, fieldName, underlyingHelper::isStoredField);
    }

    @Override
    public boolean isIndexedField(String fieldName) {
        return getFieldResult(indexedCache, fieldName, underlyingHelper::isIndexedField);
    }

    @Override
    public boolean isIndexOnlyField(String fieldName) {
        return getFieldResult(indexedOnlyCache, fieldName, underlyingHelper::isIndexOnlyField);
    }

    @Override
    public boolean isReverseIndexedField(String fieldName) {
        return getFieldResult(reverseIndexCache, fieldName, underlyingHelper::isReverseIndexedField);
    }

    @Override
    public boolean isTokenizedField(String fieldName) {
        return getFieldResult(tokenizedCache, fieldName, underlyingHelper::isTokenizedField);
    }

    @Override
    public boolean isReverseTokenizedField(String fieldName) {
        return getFieldResult(reverseTokenizedCache, fieldName, underlyingHelper::isReverseTokenizedField);
    }

    @VisibleForTesting
    boolean getFieldResult(Map<String,Boolean> resultCache, String fieldName, Predicate<String> fn) {
        return resultCache.computeIfAbsent(fieldName, fn::test);
    }
}
