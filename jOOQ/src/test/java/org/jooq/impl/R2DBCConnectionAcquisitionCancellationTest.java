package org.jooq.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;

/**
 * Cancelling a query while its connection is being acquired must neither run the query nor lose
 * the connection. A {@link ConnectionFactory} may signal {@code onSubscribe} late and may deliver a
 * connection after cancellation (Reactive Streams §1.8), as a pool does when a hand-over is
 * already in flight.
 */
public class R2DBCConnectionAcquisitionCancellationTest {

    @Test
    public void lateAcquisitionSubscriptionAfterCancellationIsCancelled() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        RecordingSubscriber<Integer> query = subscribe(factory);

        query.cancel();
        factory.signalOnSubscribe();

        assertTrue("The late acquisition was not cancelled", factory.acquisitionCancelled.get());
        assertFalse("The late acquisition was restarted", factory.connectionRequested.get());
    }

    @Test
    public void connectionDeliveredAfterCancellationIsClosedWithoutRunningTheQuery() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        RecordingSubscriber<Integer> query = subscribe(factory);
        factory.signalOnSubscribe();
        RecordingConnection connection = new RecordingConnection();

        query.cancel();
        assertTrue(factory.acquisitionCancelled.get());
        factory.deliver(connection);

        assertEquals("A statement ran for a cancelled query", 0, connection.statements.get());
        assertEquals(1, connection.closes.get());
        assertTrue(query.signals.isEmpty());
    }

    @Test
    public void connectionDeliveredBeforeCancellationIsClosedOnceByTheCancellation() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        RecordingSubscriber<Integer> query = subscribe(factory);
        factory.signalOnSubscribe();
        RecordingConnection connection = new RecordingConnection();

        factory.deliver(connection);
        query.cancel();

        assertEquals(1, connection.statements.get());
        assertEquals(1, connection.closes.get());
    }

    @Test
    public void completedQueryClosesItsConnectionOnce() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        RecordingSubscriber<Integer> query = subscribe(factory);
        factory.signalOnSubscribe();
        RecordingConnection connection = new RecordingConnection();
        connection.completeStatements = true;

        factory.deliver(connection);

        assertEquals(List.of("onNext 1", "onComplete"), query.signals);
        assertEquals(1, connection.closes.get());
    }

    @Test
    public void connectionDeliveryRacingCancellationIsClosedExactlyOnce() throws InterruptedException {
        for (int i = 0; i < 2_000; i++) {
            ControlledConnectionFactory factory = new ControlledConnectionFactory();
            RecordingSubscriber<Integer> query = subscribe(factory);
            factory.signalOnSubscribe();
            RecordingConnection connection = new RecordingConnection();
            CountDownLatch start = new CountDownLatch(1);

            Thread deliverer = new Thread(() -> {
                await(start);
                factory.deliver(connection);
            });
            deliverer.start();
            start.countDown();
            query.cancel();
            deliverer.join(TimeUnit.SECONDS.toMillis(10));

            assertEquals("Iteration " + i, 1, connection.closes.get());
        }
    }

    private static RecordingSubscriber<Integer> subscribe(ConnectionFactory factory) {
        DSLContext ctx = DSL.using(factory, SQLDialect.POSTGRES);
        RecordingSubscriber<Integer> subscriber = new RecordingSubscriber<>();
        ctx.query("update t set x = 1").subscribe(subscriber);
        assertNotNull(subscriber.subscription.get());
        subscriber.subscription.get().request(Long.MAX_VALUE);
        return subscriber;
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class RecordingSubscriber<T> implements Subscriber<T> {
        final AtomicReference<Subscription> subscription = new AtomicReference<>();
        final List<String>                  signals      = new CopyOnWriteArrayList<>();

        @Override
        public void onSubscribe(Subscription s) {
            subscription.set(s);
        }

        @Override
        public void onNext(T t) {
            signals.add("onNext " + t);
        }

        @Override
        public void onError(Throwable t) {
            signals.add("onError " + t);
        }

        @Override
        public void onComplete() {
            signals.add("onComplete");
        }

        void cancel() {
            subscription.get().cancel();
        }
    }

    /**
     * Completes its single acquisition only when told to, and keeps honouring the test's calls
     * after cancellation.
     */
    private static final class ControlledConnectionFactory implements ConnectionFactory {
        final AtomicBoolean                               acquisitionCancelled = new AtomicBoolean();
        final AtomicBoolean                               connectionRequested  = new AtomicBoolean();
        final AtomicReference<Subscriber<? super Connection>> acquirer         = new AtomicReference<>();

        @Override
        public Publisher<? extends Connection> create() {
            return acquirer::set;
        }

        @Override
        public ConnectionFactoryMetadata getMetadata() {
            return () -> "PostgreSQL";
        }

        void signalOnSubscribe() {
            acquirer.get().onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    connectionRequested.set(true);
                }

                @Override
                public void cancel() {
                    acquisitionCancelled.set(true);
                }
            });
        }

        void deliver(Connection connection) {
            acquirer.get().onNext(connection);
            acquirer.get().onComplete();
        }
    }

    private static final class RecordingConnection implements Connection {
        final AtomicInteger closes             = new AtomicInteger();
        final AtomicInteger statements         = new AtomicInteger();
        volatile boolean    completeStatements;

        @Override
        public Publisher<Void> close() {
            return s -> {
                s.onSubscribe(new Subscription() {
                    @Override
                    public void request(long n) {}

                    @Override
                    public void cancel() {}
                });
                closes.incrementAndGet();
                s.onComplete();
            };
        }

        @Override
        public Statement createStatement(String sql) {
            statements.incrementAndGet();
            return new RecordingStatement(completeStatements);
        }

        @Override
        public Publisher<Void> beginTransaction() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Publisher<Void> beginTransaction(TransactionDefinition definition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Publisher<Void> commitTransaction() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Batch createBatch() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Publisher<Void> createSavepoint(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isAutoCommit() {
            return true;
        }

        @Override
        public ConnectionMetadata getMetadata() {
            throw new UnsupportedOperationException();
        }

        @Override
        public IsolationLevel getTransactionIsolationLevel() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Publisher<Void> releaseSavepoint(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Publisher<Void> rollbackTransaction() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Publisher<Void> rollbackTransactionToSavepoint(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Publisher<Void> setAutoCommit(boolean autoCommit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Publisher<Void> setLockWaitTimeout(Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Publisher<Void> setStatementTimeout(Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Publisher<Boolean> validate(ValidationDepth depth) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Either never produces a result, or produces one result with a single updated row.
     */
    private static final class RecordingStatement implements Statement {
        final boolean complete;

        RecordingStatement(boolean complete) {
            this.complete = complete;
        }

        @Override
        public Statement add() {
            return this;
        }

        @Override
        public Statement bind(int index, Object value) {
            return this;
        }

        @Override
        public Statement bind(String name, Object value) {
            return this;
        }

        @Override
        public Statement bindNull(int index, Class<?> type) {
            return this;
        }

        @Override
        public Statement bindNull(String name, Class<?> type) {
            return this;
        }

        @Override
        public Publisher<? extends Result> execute() {
            return s -> s.onSubscribe(new Subscription() {
                boolean done;

                @Override
                public void request(long n) {
                    if (complete && !done) {
                        done = true;
                        s.onNext(new UpdateCountResult());
                        s.onComplete();
                    }
                }

                @Override
                public void cancel() {}
            });
        }
    }

    private static final class UpdateCountResult implements Result {
        @Override
        public Publisher<Long> getRowsUpdated() {
            return s -> s.onSubscribe(new Subscription() {
                boolean done;

                @Override
                public void request(long n) {
                    if (!done) {
                        done = true;
                        s.onNext(1L);
                        s.onComplete();
                    }
                }

                @Override
                public void cancel() {}
            });
        }

        @Override
        public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Publisher<T> map(Function<? super Readable, ? extends T> mappingFunction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Result filter(Predicate<Segment> filter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Publisher<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
            throw new UnsupportedOperationException();
        }
    }
}
