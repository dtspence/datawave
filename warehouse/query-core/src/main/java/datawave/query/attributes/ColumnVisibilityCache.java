package datawave.query.attributes;

import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.map.LRUMap;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.cache.CacheBuilder;

public interface ColumnVisibilityCache {
    ColumnVisibility get(ByteSequence bytes);

    enum CacheImpl {
        DEFAULT, LRU_SYNC, CAFFEINE, GUAVA
    }

    class Builder {
        CacheImpl cacheImpl;

        public Builder withImpl(CacheImpl implType) {
            this.cacheImpl = implType;
            return this;
        }

        public ColumnVisibilityCache build() {
            ColumnVisibilityCache cacheInstance;
            switch (cacheImpl) {
                case CAFFEINE:
                    cacheInstance = new CaffieneColumnVisibilityCache(this);
                    break;
                case GUAVA:
                    cacheInstance = new GuavaColumnVisibilityCache(this);
                    break;
                case LRU_SYNC:
                case DEFAULT:
                    cacheInstance = new LruColumnVisibilityCache();
                    break;
                default:
                    throw new IllegalArgumentException("Cache impl: " + cacheImpl);
            }
            return cacheInstance;
        }

        private CacheImpl getCacheImpl() {
            return cacheImpl;
        }
    }

    class CaffieneColumnVisibilityCache implements ColumnVisibilityCache {
        private Cache<ByteSequence,ColumnVisibility> cache;

        public CaffieneColumnVisibilityCache(Builder builder) {
            cache = Caffeine.newBuilder().initialCapacity(256).maximumSize(256).build();
        }

        @Override
        public ColumnVisibility get(ByteSequence bytes) {
            return cache.get(bytes, (k) -> new ColumnVisibility(bytes.toArray()));
        }
    }

    class GuavaColumnVisibilityCache implements ColumnVisibilityCache {
        private com.google.common.cache.Cache<ByteSequence,ColumnVisibility> cache;

        public GuavaColumnVisibilityCache(Builder builder) {
            cache = CacheBuilder.newBuilder().initialCapacity(256).maximumSize(256).build();
        }

        @Override
        public ColumnVisibility get(ByteSequence bytes) {
            try {
                return cache.get(bytes, () -> new ColumnVisibility(bytes.toArray()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    class LruColumnVisibilityCache implements ColumnVisibilityCache {
        private Map<ByteSequence,ColumnVisibility> cache = Collections.synchronizedMap(new LRUMap<>(256));

        public ColumnVisibility get(ByteSequence bytes) {
            return cache.computeIfAbsent(bytes, (k) -> new ColumnVisibility(bytes.toArray()));
        }
    }
}
