package org.jooq.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;

/**
 * A query cancelled from another thread while it is set up on its connection must not have that
 * connection closed under it. A pool returns a closed connection to other users, so a statement
 * executed after the close would run on someone else's connection, and r2dbc-pool fails the
 * statement's creation instead ("The connection is closed"). Such a cancellation must return promptly,
 * with the connection closed once setup returns. A cancellation on the setup thread itself proceeds as
 * upstream.
 */
public class R2DBCCancellationDuringSetupTest {

    @Test
    public void cancellationDuringSetupClosesTheConnectionAfterTheStatementIsExecuted() throws InterruptedException {
        PoolLikeConnection connection = new PoolLikeConnection();
        RecordingSubscriber query = new RecordingSubscriber();
        CountDownLatch inSetup = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        Thread canceller = new Thread(() -> {
            await(inSetup);
            query.subscription.get().cancel();
            cancelled.countDown();
        });
        connection.onSetup = () -> {
            inSetup.countDown();
            await(cancelled);
        };
        canceller.start();

        execute(connection.connection(), query);
        canceller.join(TimeUnit.SECONDS.toMillis(10));

        assertEquals(List.of("createStatement", "execute", "close"), connection.events);
        assertTrue(query.signals.toString(), query.signals.isEmpty());
    }

    /**
     * r2dbc-pool releases the connection of an acquisition cancelled before it completes, even after
     * delivering it, so a transaction would keep using a connection other subscribers acquire.
     */
    @Test
    public void cancellationDuringSetupDoesNotCancelTheDeliveredConnectionsAcquisition() throws InterruptedException {
        PoolLikeConnection connection = new PoolLikeConnection();
        RecordingSubscriber query = new RecordingSubscriber();
        AtomicBoolean acquisitionCancelled = new AtomicBoolean();
        CountDownLatch inSetup = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        Thread canceller = new Thread(() -> {
            await(inSetup);
            query.subscription.get().cancel();
            cancelled.countDown();
        });
        connection.onSetup = () -> {
            inSetup.countDown();
            await(cancelled);
        };
        canceller.start();

        subscribe(s -> s.onSubscribe(new Subscription() {
            final AtomicBoolean delivered = new AtomicBoolean();

            @Override
            public void request(long n) {
                if (!delivered.getAndSet(true)) {
                    s.onNext(connection.connection());
                    s.onComplete();
                }
            }

            @Override
            public void cancel() {
                acquisitionCancelled.set(true);
            }
        }), query);
        query.subscription.get().request(Long.MAX_VALUE);
        canceller.join(TimeUnit.SECONDS.toMillis(10));

        assertFalse("The delivered connection's acquisition was cancelled", acquisitionCancelled.get());
        assertEquals(List.of("createStatement", "execute", "close"), connection.events);
    }

    @Test
    public void setupFailureClosesTheConnectionBeforeSignallingTheErrorAsUpstream() {
        PoolLikeConnection connection = new PoolLikeConnection();
        RecordingSubscriber query = new RecordingSubscriber();
        AtomicBoolean closedBeforeOnError = new AtomicBoolean();
        connection.onSetup = () -> { throw new IllegalStateException("setup failed"); };
        query.onError = () -> closedBeforeOnError.set(connection.closed.get());

        execute(connection.connection(), query);

        assertEquals(List.of("close"), connection.events);
        assertEquals(1, query.signals.size());
        assertTrue(query.signals.get(0), query.signals.get(0).startsWith("onError "));
        assertTrue("The connection is closed before onError", closedBeforeOnError.get());
    }

    @Test
    public void synchronousCompletionOnTheSetupThreadClosesTheConnectionAsUpstream() {
        PoolLikeConnection connection = new PoolLikeConnection();
        RecordingSubscriber query = new RecordingSubscriber();
        connection.completeStatements = true;

        execute(connection.connection(), query);

        assertEquals(List.of("createStatement", "execute", "close", "executeReturned"), connection.events);
        assertEquals(List.of("onNext 1", "onComplete"), query.signals);
    }

    @Test
    public void cancellationDoesNotDeadlockWithASynchronousDownstreamCallback() throws InterruptedException {
        PoolLikeConnection connection = new PoolLikeConnection();
        RecordingSubscriber query = new RecordingSubscriber();
        AtomicReference<Subscriber<? super Connection>> acquisition = new AtomicReference<>();
        Object applicationLock = new Object();
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch inCallback = new CountDownLatch(1);
        connection.completeStatements = true;
        query.onNext = () -> {
            inCallback.countDown();
            synchronized (applicationLock) {}
        };
        subscribe(s -> {
            acquisition.set(s);
            s.onSubscribe(new NoOpSubscription());
        }, query);
        query.subscription.get().request(Long.MAX_VALUE);

        Thread canceller = new Thread(() -> {
            synchronized (applicationLock) {
                locked.countDown();
                await(inCallback);
                query.subscription.get().cancel();
            }
        });
        Thread deliverer = new Thread(() -> acquisition.get().onNext(connection.connection()));
        // A regression must fail within a bounded time without preventing the test JVM from exiting.
        canceller.setDaemon(true);
        deliverer.setDaemon(true);
        canceller.start();
        await(locked);
        deliverer.start();
        canceller.join(TimeUnit.SECONDS.toMillis(5));
        deliverer.join(TimeUnit.SECONDS.toMillis(5));

        assertFalse("Cancellation is waiting for the downstream callback", canceller.isAlive());
        assertFalse("The downstream callback is waiting for cancellation", deliverer.isAlive());
        assertEquals(List.of("createStatement", "execute", "executeReturned", "close"), connection.events);
        assertEquals(List.of("onNext 1"), query.signals);
    }

    @Test
    public void cancellationDuringBothBatchSetupsDefersClosingUntilExecution() throws InterruptedException {
        for (boolean singleStatement : List.of(false, true)) {
            PoolLikeConnection connection = new PoolLikeConnection();
            RecordingSubscriber query = new RecordingSubscriber();
            CountDownLatch inSetup = new CountDownLatch(1);
            CountDownLatch cancelled = new CountDownLatch(1);
            Thread canceller = new Thread(() -> {
                await(inSetup);
                query.subscription.get().cancel();
                cancelled.countDown();
            });
            connection.onSetup = () -> {
                inSetup.countDown();
                await(cancelled);
            };
            DSLContext ctx = DSL.using(factory(just(connection.connection())), SQLDialect.POSTGRES);
            org.jooq.Batch batch = singleStatement
                ? ctx.batch(ctx.query("update t set x = ?", 0)).bind(1).bind(2)
                : ctx.batch(ctx.query("update t set x = 1"), ctx.query("update t set x = 2"));
            batch.subscribe(query);
            canceller.start();

            query.subscription.get().request(Long.MAX_VALUE);
            canceller.join(TimeUnit.SECONDS.toMillis(10));

            assertEquals(singleStatement
                ? List.of("createStatement", "add", "execute", "close")
                : List.of("createBatch", "add", "add", "execute", "close"), connection.events);
            assertTrue(query.signals.toString(), query.signals.isEmpty());
        }
    }

    @Test
    public void cancellationAndCompletionDoNotCloseAnExternallyOwnedConnection() {
        for (boolean complete : List.of(false, true)) {
            PoolLikeConnection connection = new PoolLikeConnection();
            RecordingSubscriber query = new RecordingSubscriber();
            connection.completeStatements = complete;
            if (!complete)
                connection.onSetup = () -> query.subscription.get().cancel();
            DSL.using(connection.connection(), SQLDialect.POSTGRES).query("update t set x = ?", 1).subscribe(query);

            query.subscription.get().request(Long.MAX_VALUE);

            assertEquals(complete
                ? List.of("createStatement", "execute", "executeReturned")
                : List.of("createStatement", "execute"), connection.events);
            assertEquals(complete ? List.of("onNext 1", "onComplete") : List.of(), query.signals);
            assertFalse("The caller owns the connection", connection.closed.get());
        }
    }

    @Test
    public void racingCancellationNeverClosesTheConnectionBeforeTheStatementIsExecuted() throws InterruptedException {
        for (int i = 0; i < 2_000; i++) {
            PoolLikeConnection connection = new PoolLikeConnection();
            RecordingSubscriber query = new RecordingSubscriber();
            CountDownLatch start = new CountDownLatch(1);
            Thread canceller = new Thread(() -> {
                await(start);
                query.subscription.get().cancel();
            });
            canceller.start();

            subscribe(connection.connection(), query);
            start.countDown();
            query.subscription.get().request(Long.MAX_VALUE);
            canceller.join(TimeUnit.SECONDS.toMillis(10));

            // A cancellation before the request acquires no connection at all
            List<String> events = connection.events;
            assertEquals("Iteration " + i + " " + events, events.isEmpty() ? 0 : 1, events.stream().filter("close"::equals).count());
            assertTrue("Iteration " + i + " " + events, !events.contains("execute") || events.indexOf("execute") < events.indexOf("close"));
            assertTrue("Iteration " + i + " " + query.signals, query.signals.isEmpty());
        }
    }

    private static void execute(Connection connection, RecordingSubscriber query) {
        subscribe(connection, query);
        query.subscription.get().request(Long.MAX_VALUE);
    }

    private static void subscribe(Connection connection, RecordingSubscriber query) {
        subscribe(just(connection), query);
    }

    private static void subscribe(Publisher<? extends Connection> connection, RecordingSubscriber query) {
        DSL.using(factory(connection), SQLDialect.POSTGRES).query("update t set x = ?", 1).subscribe(query);
    }

    private static ConnectionFactory factory(Publisher<? extends Connection> connection) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return connection;
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };
    }

    private static <T> Publisher<T> just(T value) {
        return s -> s.onSubscribe(new Subscription() {
            final AtomicBoolean delivered = new AtomicBoolean();

            @Override
            public void request(long n) {
                if (!delivered.getAndSet(true)) {
                    s.onNext(value);
                    s.onComplete();
                }
            }

            @Override
            public void cancel() {}
        });
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue("Timed out waiting for the other thread", latch.await(5, TimeUnit.SECONDS));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static final class RecordingSubscriber implements Subscriber<Integer> {
        final AtomicReference<Subscription> subscription = new AtomicReference<>();
        final List<String>                  signals      = new CopyOnWriteArrayList<>();
        volatile Runnable                   onNext       = () -> {};
        volatile Runnable                   onError      = () -> {};

        @Override
        public void onSubscribe(Subscription s) {
            subscription.set(s);
        }

        @Override
        public void onNext(Integer rows) {
            signals.add("onNext " + rows);
            onNext.run();
        }

        @Override
        public void onError(Throwable t) {
            signals.add("onError " + t);
            onError.run();
        }

        @Override
        public void onComplete() {
            signals.add("onComplete");
        }
    }

    /**
     * Fails after {@code close()} like r2dbc-pool's {@code PooledConnection}, and records the
     * calls that matter for the order of execution and close. Statements may complete synchronously.
     */
    private static final class PoolLikeConnection {
        final List<String>  events  = new CopyOnWriteArrayList<>();
        final AtomicBoolean closed  = new AtomicBoolean();
        volatile Runnable   onSetup = () -> {};
        volatile boolean    completeStatements;

        Connection connection() {
            return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] { Connection.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "createStatement", "createBatch" -> {
                        onSetup.run();

                        if (closed.get())
                            throw new IllegalStateException("The connection is closed");

                        events.add(method.getName());
                        yield method.getName().equals("createStatement") ? statement() : batch();
                    }
                    case "close" -> (Publisher<Void>) s -> {
                        s.onSubscribe(new NoOpSubscription());
                        closed.set(true);
                        events.add("close");
                        s.onComplete();
                    };
                    case "toString" -> "connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                }
            );
        }

        private Batch batch() {
            return new Batch() {
                @Override
                public Batch add(String sql) {
                    events.add("add");
                    return this;
                }

                @Override
                public Publisher<? extends Result> execute() {
                    return statement().execute();
                }
            };
        }

        private Statement statement() {
            return (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[] { Statement.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "bind", "bindNull" -> proxy;
                    case "add" -> {
                        events.add("add");
                        yield proxy;
                    }
                    case "execute" -> (Publisher<Result>) s -> {
                        events.add("execute");
                        if (completeStatements) {
                            Result result = (Result) Proxy.newProxyInstance(
                                Result.class.getClassLoader(),
                                new Class<?>[] { Result.class },
                                (p, m, a) -> {
                                    if (m.getName().equals("getRowsUpdated"))
                                        return just(1L);
                                    throw new UnsupportedOperationException(m.getName());
                                }
                            );
                            just(result).subscribe(s);
                            events.add("executeReturned");
                        }
                        else
                            s.onSubscribe(new NoOpSubscription());
                    };
                    case "toString" -> "statement";
                    default -> throw new UnsupportedOperationException(method.getName());
                }
            );
        }
    }

    private static final class NoOpSubscription implements Subscription {
        @Override
        public void request(long n) {}

        @Override
        public void cancel() {}
    }
}
