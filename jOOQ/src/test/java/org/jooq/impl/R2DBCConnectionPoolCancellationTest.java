package org.jooq.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.jooq.SQLDialect;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Mono;

/**
 * Cancellations against a real r2dbc-pool {@link ConnectionPool} of a single connection, which is
 * a mock, so no database is needed. While jOOQ still uses the connection, the pool must not hand
 * it out to anyone else.
 */
public class R2DBCConnectionPoolCancellationTest {

    @Test
    public void queryCancelledDuringSetupKeepsItsConnectionUntilTheStatementIsExecuted() throws InterruptedException {
        MockConnection connection = new MockConnection();
        ConnectionPool pool = pool(connection);
        RecordingSubscriber query = new RecordingSubscriber();
        AtomicReference<Connection> handedOut = new AtomicReference<>();
        CountDownLatch inSetup = new CountDownLatch(1);
        CountDownLatch checked = new CountDownLatch(1);
        Thread canceller = new Thread(() -> {
            await(inSetup);
            query.subscription.get().cancel();
            handedOut.set(tryAcquire(pool));
            checked.countDown();
        });
        connection.onCreateStatement = () -> {
            inSetup.countDown();
            await(checked);
        };

        try {
            canceller.start();
            DSL.using(pool, SQLDialect.POSTGRES).query("update t set x = 1").subscribe(query);
            query.subscription.get().request(Long.MAX_VALUE);
            canceller.join(TimeUnit.SECONDS.toMillis(10));

            assertNull("The pool handed out the connection while a statement was being set up on it", handedOut.get());

            // The connection returns to the pool once the setup has executed the statement
            assertNotNull("The connection did not return to the pool", release(acquire(pool, Duration.ofSeconds(10))));
            assertEquals(query.signals.toString(), List.of("createStatement", "execute"), connection.events);
        }
        finally {
            release(handedOut.get());
            pool.disposeLater().block();
        }
    }

    @Test
    public void transactionCancelledDuringSetupKeepsItsConnectionUntilItEnds() throws InterruptedException {
        MockConnection connection = new MockConnection();
        ConnectionPool pool = pool(connection);
        RecordingSubscriber transaction = new RecordingSubscriber();
        CountDownLatch inSetup = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        Thread canceller = new Thread(() -> {
            await(inSetup);
            transaction.subscription.get().cancel();
            cancelled.countDown();
        });
        connection.onBeginTransaction = () -> {
            inSetup.countDown();
            await(cancelled);
        };
        Connection handedOut = null;

        try {
            canceller.start();
            DSL.using(pool, SQLDialect.POSTGRES)
               .transactionPublisher(trx -> trx.dsl().query("update t set x = 1"))
               .subscribe(transaction);
            transaction.subscription.get().request(Long.MAX_VALUE);
            canceller.join(TimeUnit.SECONDS.toMillis(10));

            // The transaction has begun but not ended, so it still uses the pool's only connection
            handedOut = tryAcquire(pool);
            assertNull("The pool handed out the connection of a transaction still using it", handedOut);

            connection.completeBegin();
            assertNotNull("The connection did not return to the pool", release(acquire(pool, Duration.ofSeconds(10))));
            assertEquals(List.of("beginTransaction", "createStatement", "execute", "commitTransaction"), connection.events);
        }
        finally {
            release(handedOut);
            pool.disposeLater().block();
        }
    }

    private static ConnectionPool pool(MockConnection connection) {
        ConnectionFactory factory = new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.just(connection.connection());
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };

        return new ConnectionPool(ConnectionPoolConfiguration.builder(factory).initialSize(0).maxSize(1).acquireRetry(0).build());
    }

    /**
     * Acquires a connection unless the pool has none to hand out.
     */
    private static Connection tryAcquire(ConnectionPool pool) {
        return acquire(pool, Duration.ofMillis(300));
    }

    private static Connection acquire(ConnectionPool pool, Duration timeout) {
        return Mono.from(pool.create())
            .timeout(timeout)
            .onErrorResume(TimeoutException.class, e -> Mono.empty())
            .block();
    }

    private static Connection release(Connection connection) {
        if (connection != null)
            Mono.from(connection.close()).block();

        return connection;
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS))
                throw new AssertionError("Timed out waiting for the other thread");
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static final class RecordingSubscriber implements Subscriber<Integer> {
        final AtomicReference<Subscription> subscription = new AtomicReference<>();
        final List<String>                  signals      = new CopyOnWriteArrayList<>();

        @Override
        public void onSubscribe(Subscription s) {
            subscription.set(s);
        }

        @Override
        public void onNext(Integer rows) {
            signals.add("onNext " + rows);
        }

        @Override
        public void onError(Throwable t) {
            signals.add("onError " + t);
        }

        @Override
        public void onComplete() {
            signals.add("onComplete");
        }
    }

    /**
     * Records the calls that matter, can pause in {@code createStatement()} and
     * {@code beginTransaction()}, and holds {@code BEGIN} open until {@link #completeBegin()}.
     */
    private static final class MockConnection {
        final List<String>                        events             = new CopyOnWriteArrayList<>();
        final AtomicReference<Subscriber<? super Void>> begin        = new AtomicReference<>();
        volatile Runnable                         onCreateStatement  = () -> {};
        volatile Runnable                         onBeginTransaction = () -> {};

        Connection connection() {
            return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] { Connection.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "beginTransaction" -> (Publisher<Void>) s -> s.onSubscribe(once(() -> {
                        events.add("beginTransaction");
                        onBeginTransaction.run();
                        begin.set(s);
                    }));
                    case "commitTransaction", "rollbackTransaction" -> (Publisher<Void>) s -> s.onSubscribe(once(() -> {
                        events.add(method.getName());
                        s.onComplete();
                    }));
                    case "createStatement" -> {
                        events.add("createStatement");
                        onCreateStatement.run();
                        yield statement();
                    }
                    case "validate" -> Mono.just(true);
                    case "isAutoCommit" -> true;
                    case "close" -> Mono.empty();
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                }
            );
        }

        void completeBegin() {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

            while (begin.get() == null) {
                if (System.nanoTime() > deadline)
                    throw new AssertionError("BEGIN was not requested");

                Thread.onSpinWait();
            }

            begin.get().onComplete();
        }

        private Statement statement() {
            return (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[] { Statement.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "bind", "bindNull", "add", "fetchSize", "returnGeneratedValues" -> proxy;
                    case "execute" -> {
                        events.add("execute");
                        yield Mono.empty();
                    }
                    case "toString" -> "statement";
                    default -> throw new UnsupportedOperationException(method.getName());
                }
            );
        }

        private static Subscription once(Runnable request) {
            AtomicBoolean requested = new AtomicBoolean();

            return new Subscription() {
                @Override
                public void request(long n) {
                    if (!requested.getAndSet(true))
                        request.run();
                }

                @Override
                public void cancel() {}
            };
        }
    }
}
