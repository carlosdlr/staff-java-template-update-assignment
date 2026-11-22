package com.caseware.fanout;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PostgresFanoutSimulationTest {
    @Container static final PostgreSQLContainer<?> DB = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("fanout").withUsername("fanout").withPassword("fanout");

    @BeforeEach void schema() throws Exception {
        try (var c = db(); var s = c.createStatement();
             InputStream in = getClass().getResourceAsStream("/db/simulation-schema.sql")) {
            s.execute("DROP SCHEMA public CASCADE");
            s.execute("CREATE SCHEMA public");
            if (in == null) throw new IllegalStateException("missing simulation schema");
            for (var sql : new String(in.readAllBytes(), StandardCharsets.UTF_8).split(";"))
                if (!sql.isBlank()) s.execute(sql);
        }
    }

    @Test void keysetPaginationAndRedeliveryAreIdempotent() throws Exception {
        seed(7); var checkpoints = new Checkpoints(); var dlq = new Dlq(); var refresh = new Refresh();
        try (var worker = worker(2, 3, new Gate(2), refresh, checkpoints, dlq)) {
            worker.process(event("publish-1")); worker.process(event("publish-1"));
        }
        assertEquals(7, count("refresh_ledger")); assertTrue(completed("publish-1"));
        assertTrue(refresh.peak.get() <= 2); assertEquals(0, count("dead_letter"));
    }

    @Test void transientFailureRetriesAndEventuallyCheckpoints() throws Exception {
        seed(1); var calls = new AtomicInteger();
        TemplatePublishWorker.PendingRefresh downstream = (event, ref, key) -> {
            if (calls.incrementAndGet() == 1) {
                throw new TemplatePublishWorker.PendingRefreshException("simulated timeout",
                        new SQLException("simulated timeout"));
            }
            saveRefresh(event, ref, key);
        };
        try (var worker = worker(1, 1, new Gate(1), downstream, new Checkpoints(), new Dlq())) {
            worker.process(event("publish-retry"));
        }
        assertEquals(2, calls.get()); assertEquals(1, count("refresh_ledger"));
        assertTrue(completed("publish-retry"));
    }

    @Test void permanentFailureIsDeadLetteredAndBatchContinues() throws Exception {
        seed(2);
        TemplatePublishWorker.PendingRefresh downstream = (event, ref, key) -> {
            if (ref.engagementId().endsWith("0001")) throw new TemplatePublishWorker.NonRetryableFailure("invalid ref");
            saveRefresh(event, ref, key);
        };
        try (var worker = worker(1, 2, new Gate(1), downstream, new Checkpoints(), new Dlq())) {
            worker.process(event("publish-dlq"));
        }
        assertEquals(1, count("dead_letter")); assertEquals(1, count("refresh_ledger"));
        assertTrue(completed("publish-dlq"));
    }

    @Test void capacityPressureDefersWithoutAdvancingCursorOrDeadLettering() throws Exception {
        seed(1); var checkpoints = new Checkpoints();
        TemplatePublishWorker.CapacityGate exhausted = _ -> {
            throw new TemplatePublishWorker.CapacityUnavailableException("quota exhausted");
        };
        try (var worker = worker(1, 1, exhausted, new Refresh(), checkpoints, new Dlq())) {
            assertThrows(TemplatePublishWorker.CapacityUnavailableException.class,
                    () -> worker.process(event("publish-capacity")));
        }
        assertNull(checkpoints.load("publish-capacity")); assertFalse(completed("publish-capacity"));
        assertEquals(0, count("dead_letter")); assertEquals(0, count("refresh_ledger"));
    }

    private TemplatePublishWorker worker(int concurrency, int pageSize, TemplatePublishWorker.CapacityGate gate,
            TemplatePublishWorker.PendingRefresh refresh, TemplatePublishWorker.CheckpointStore checkpoint,
            TemplatePublishWorker.DeadLetter dlq) {
        return new TemplatePublishWorker(new Index(), gate, refresh, checkpoint, dlq,
                new TemplatePublishWorker.Limits(concurrency, pageSize, 3, Duration.ofSeconds(1), Duration.ZERO));
    }
    private static TemplatePublishWorker.Publish event(String id) {
        return new TemplatePublishWorker.Publish(id, "product-1", "CA", "v7");
    }
    private void seed(int n) throws SQLException {
        try (var c = db(); var s = c.prepareStatement("INSERT INTO engagement_reference VALUES ('ca-central-1','product-1','CA','firm-1',?,'v4')")) {
            for (int i=1;i<=n;i++) { s.setString(1,"eng-%04d".formatted(i)); s.addBatch(); }
            s.executeBatch();
        }
    }
    private static Connection db() throws SQLException { return DriverManager.getConnection(DB.getJdbcUrl(),DB.getUsername(),DB.getPassword()); }
    private int count(String table) throws SQLException {
        try (var c=db(); var s=c.createStatement(); var r=s.executeQuery("SELECT count(*) FROM "+table)) { r.next(); return r.getInt(1); }
    }
    private boolean completed(String event) throws SQLException {
        try(var c=db();var s=c.prepareStatement("SELECT completed FROM publish_checkpoint WHERE event_id=?")) {
            s.setString(1,event);try(var r=s.executeQuery()){return r.next()&&r.getBoolean(1);}
        }
    }
    private static void saveRefresh(TemplatePublishWorker.Publish event, TemplatePublishWorker.EngagementRef ref, String key) {
        try(var c=db();var s=c.prepareStatement("INSERT INTO refresh_ledger VALUES(?,?,?) ON CONFLICT(idempotency_key) DO NOTHING")) {
            s.setString(1,key);s.setString(2,event.eventId());s.setString(3,ref.engagementId());s.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("could not record refresh", e);
        }
    }

    private static final class Index implements TemplatePublishWorker.EngagementIndex {
        public TemplatePublishWorker.Page findPage(String product,String market,String cursor,int limit) {
            try(var c=db();var s=c.prepareStatement("""
                SELECT firm_id,engagement_id,applied_version FROM engagement_reference
                WHERE region='ca-central-1' AND product_id=? AND market=? AND (CAST(? AS text) IS NULL OR engagement_id>?)
                ORDER BY engagement_id LIMIT ?
                """)) {
                s.setString(1,product);s.setString(2,market);s.setString(3,cursor);s.setString(4,cursor);s.setInt(5,limit+1);
                try(var r=s.executeQuery()) { var rows=new ArrayList<TemplatePublishWorker.EngagementRef>();
                    while(r.next())rows.add(new TemplatePublishWorker.EngagementRef(r.getString(1),r.getString(2),r.getString(3)));
                    boolean more=rows.size()>limit;if(more)rows.removeLast();
                    return new TemplatePublishWorker.Page(rows,more?rows.getLast().engagementId():null);
                }
            } catch(SQLException e){throw new IllegalStateException(e);}
        }
    }
    private static final class Checkpoints implements TemplatePublishWorker.CheckpointStore {
        public String load(String id){try(var c=db();var s=c.prepareStatement("SELECT cursor FROM publish_checkpoint WHERE event_id=?")){
            s.setString(1,id);try(var r=s.executeQuery()){return r.next()?r.getString(1):null;}
        }catch(SQLException e){throw new IllegalStateException(e);}}
        public void save(String id,String cursor){upsert(id,cursor,false);}
        public void complete(String id){upsert(id,load(id),true);}
        private void upsert(String id,String cursor,boolean done){try(var c=db();var s=c.prepareStatement("""
            INSERT INTO publish_checkpoint VALUES(?,?,?) ON CONFLICT(event_id) DO UPDATE SET cursor=excluded.cursor,completed=excluded.completed
            """)){s.setString(1,id);s.setString(2,cursor);s.setBoolean(3,done);s.executeUpdate();}catch(SQLException e){throw new IllegalStateException(e);}}
    }
    private static final class Refresh implements TemplatePublishWorker.PendingRefresh {
        final AtomicInteger active=new AtomicInteger(),peak=new AtomicInteger();
        public void refreshPending(TemplatePublishWorker.Publish event,TemplatePublishWorker.EngagementRef ref,String key){
            peak.accumulateAndGet(active.incrementAndGet(),Math::max);try{saveRefresh(event,ref,key);}finally{active.decrementAndGet();}
        }
    }
    private static final class Dlq implements TemplatePublishWorker.DeadLetter {
        public void recordFailure(TemplatePublishWorker.Publish event,TemplatePublishWorker.EngagementRef ref,String key,String reason){
            try(var c=db();var s=c.prepareStatement("INSERT INTO dead_letter VALUES(?,?,?,?) ON CONFLICT(idempotency_key) DO NOTHING")){
                s.setString(1,key);s.setString(2,event.eventId());s.setString(3,ref.engagementId());s.setString(4,reason);s.executeUpdate();
            }catch(SQLException e){throw new IllegalStateException(e);}
        }
    }
    private static final class Gate implements TemplatePublishWorker.CapacityGate {
        final Semaphore availableLeases;Gate(int count){availableLeases=new Semaphore(count);}
        public Permit acquire(Duration timeout)throws InterruptedException,TemplatePublishWorker.CapacityUnavailableException{
            if(!availableLeases.tryAcquire(timeout.toMillis(),java.util.concurrent.TimeUnit.MILLISECONDS))throw new TemplatePublishWorker.CapacityUnavailableException("quota exhausted");
            return availableLeases::release;
        }
    }
}
