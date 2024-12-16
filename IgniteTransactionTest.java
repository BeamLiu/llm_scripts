package emop.sampleapp;

import lombok.Data;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import java.util.List;

public class IgniteTransactionTest {
    private static final String CACHE_NAME = "testCache";

    public static void main(String[] args) {
        try (Ignite ignite = startIgnite()) {
            // create cache
            IgniteCache<Long, TestEntity> cache = getOrCreateCache(ignite);

            // test data visibility within transaction
            testTransactionVisibility(ignite, cache);
        }
    }

    private static Ignite startIgnite() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(false);
        return Ignition.start(cfg);
    }

    private static IgniteCache<Long, TestEntity> getOrCreateCache(Ignite ignite) {
        CacheConfiguration<Long, TestEntity> cacheCfg = new CacheConfiguration<>(CACHE_NAME);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setCacheMode(CacheMode.REPLICATED);

        cacheCfg.setIndexedTypes(Long.class, TestEntity.class);

        return ignite.getOrCreateCache(cacheCfg);
    }

    private static void testTransactionVisibility(Ignite ignite, IgniteCache<Long, TestEntity> cache) {
        Long id = System.currentTimeMillis();
        TestEntity entity = new TestEntity(id, "Test Entity");

        try (Transaction tx = ignite.transactions().txStart(
                TransactionConcurrency.OPTIMISTIC,
                TransactionIsolation.REPEATABLE_READ)) {

            System.out.println("Starting transaction...");

            cache.put(id, entity);
            System.out.println("Entity saved in transaction");

            TestEntity fromCache = cache.get(id);
            System.out.println("Direct cache get result: " + fromCache);

            SqlFieldsQuery query = new SqlFieldsQuery(
                    "SELECT _key, name FROM TestEntity WHERE _key = ?"
            ).setArgs(id);

            List<List<?>> queryResult = cache.query(query).getAll();
            System.out.println("Expect to have result, but SQL query result is empty : " + queryResult);

            tx.commit();
        }
    }

    // Entity
    @Data
    public static class TestEntity {
        @QuerySqlField
        private final Long id;
        @QuerySqlField
        private final String name;
    }
}

