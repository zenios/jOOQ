package org.jooq.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.jooq.SQLDialect;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;

public class R2DBCDeferredTransactionCleanupTest {

    @Test
    public void setupCancellationKeepsTheAcquisitionAndWaitsForClose() {
        for (boolean rollback : new boolean[] { false, true })
            assertDeferredCleanup(rollback, false);
    }

    // r2dbc-pool releases the connection of a cancelled acquisition, while the transaction still uses it
    @Test
    public void deliveredConnectionsAcquisitionIsNeverCancelled() {
        for (boolean rollback : new boolean[] { false, true })
            assertDeferredCleanup(rollback, true);
    }

    private static void assertDeferredCleanup(boolean rollback, boolean cancelThrows) {
        RuntimeException cancelFailure = new RuntimeException("acquisition cancel failed");
        RuntimeException transactionFailure = new RuntimeException("transaction failed");
        List<String> events = new ArrayList<>();
        AtomicReference<Subscription> transaction = new AtomicReference<>();
        AtomicReference<Subscriber<? super Void>> close = new AtomicReference<>();
        String terminal = rollback ? "onError" : "onComplete";
        String finish = rollback ? "rollbackTransaction" : "commitTransaction";
        Connection connection = (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(), new Class<?>[] { Connection.class },
            (proxy, method, args) -> switch (method.getName()) {
                case "beginTransaction" -> (Publisher<Void>) s ->
                    s.onSubscribe(once(() -> {
                        events.add("beginTransaction");

                        // The transaction begins, runs and finishes on the driver's thread before setup returns
                        Thread driver = new Thread(s::onComplete);
                        driver.setDaemon(true);
                        driver.start();
                        join(driver);
                        events.add("beginTransaction.return");
                    }));
                case "commitTransaction", "rollbackTransaction" -> (Publisher<Void>) s ->
                    s.onSubscribe(once(() -> {
                        events.add(method.getName());
                        s.onComplete();
                        events.add(method.getName() + ".return");
                    }));
                case "close" -> (Publisher<Void>) s -> {
                    events.add("close");
                    close.set(s);
                    s.onSubscribe(once(() -> {}));
                };
                case "toString" -> "transaction-connection";
                default -> throw new UnsupportedOperationException(method.getName());
            }
        );
        ConnectionFactory factory = new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return s -> s.onSubscribe(new Subscription() {
                    boolean requested;

                    @Override
                    public void request(long n) {
                        if (!requested) {
                            requested = true;
                            s.onNext(connection);
                            s.onComplete();
                        }
                    }

                    @Override
                    public void cancel() {
                        events.add("cancelAcquisition");
                        if (cancelThrows)
                            throw cancelFailure;
                    }
                });
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };

        DSL.using(factory, SQLDialect.POSTGRES).<Integer>transactionPublisher(configuration -> s ->
            s.onSubscribe(once(() -> {
                try {
                    transaction.get().cancel();
                    events.add("cancel.return");
                }
                finally {
                    // Both cancellation and transaction completion happen on the driver's thread before setup returns.
                    if (rollback)
                        s.onError(transactionFailure);
                    else
                        s.onComplete();
                    events.add("transaction.return");
                }
            }))
        ).subscribe(new Subscriber<Integer>() {
            @Override
            public void onSubscribe(Subscription s) {
                transaction.set(s);
            }

            @Override
            public void onNext(Integer value) {
                fail("Unexpected transaction result");
            }

            @Override
            public void onError(Throwable t) {
                assertSame(transactionFailure, t);
                events.add("onError");
            }

            @Override
            public void onComplete() {
                events.add("onComplete");
            }
        });

        transaction.get().request(1);

        List<String> beforeClose = List.of("beginTransaction", "cancel.return", finish, finish + ".return",
            "transaction.return", "beginTransaction.return", "close");
        assertEquals(beforeClose, events);
        assertNotNull(close.get());

        close.get().onComplete();

        List<String> afterClose = new ArrayList<>(beforeClose);
        afterClose.add(terminal);
        assertEquals(afterClose, events);
    }

    private static Subscription once(Runnable request) {
        return new Subscription() {
            boolean requested;

            @Override
            public void request(long n) {
                if (!requested) {
                    requested = true;
                    request.run();
                }
            }

            @Override
            public void cancel() {}
        };
    }

    private static void join(Thread thread) {
        try {
            thread.join(TimeUnit.SECONDS.toMillis(10));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }

        if (thread.isAlive())
            throw new AssertionError("The transaction did not finish");
    }
}
