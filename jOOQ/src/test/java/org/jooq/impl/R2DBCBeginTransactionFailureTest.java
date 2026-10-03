package org.jooq.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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

public class R2DBCBeginTransactionFailureTest {

    // A pool takes its connection back only when it is closed, so a failed BEGIN that is not closed is lost for good
    @Test
    public void failedBeginClosesTheConnectionBeforeSignallingTheFailure() {
        for (boolean onDriverThread : new boolean[] { false, true })
            assertFailedBeginIsClosed(onDriverThread);
    }

    private static void assertFailedBeginIsClosed(boolean onDriverThread) {
        RuntimeException beginFailure = new RuntimeException("begin failed");
        List<String> events = new CopyOnWriteArrayList<>();
        AtomicReference<Subscriber<? super Void>> close = new AtomicReference<>();
        Connection connection = (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(), new Class<?>[] { Connection.class },
            (proxy, method, args) -> switch (method.getName()) {
                case "beginTransaction" -> (Publisher<Void>) s ->
                    s.onSubscribe(once(() -> {
                        events.add("beginTransaction");
                        if (onDriverThread) {
                            Thread driver = new Thread(() -> s.onError(beginFailure));
                            driver.setDaemon(true);
                            driver.start();
                            join(driver);
                        }
                        else
                            s.onError(beginFailure);
                    }));
                case "close" -> (Publisher<Void>) s -> {
                    events.add("close");
                    close.set(s);
                    s.onSubscribe(once(() -> {}));
                };
                case "toString" -> "begin-failure-connection";
                default -> throw new UnsupportedOperationException(method.getName());
            }
        );
        ConnectionFactory factory = new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return s -> s.onSubscribe(once(() -> {
                    s.onNext(connection);
                    s.onComplete();
                }));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };

        DSL.using(factory, SQLDialect.POSTGRES).<Integer>transactionPublisher(configuration -> {
            events.add("transaction");
            return s -> s.onSubscribe(once(s::onComplete));
        }).subscribe(new Subscriber<Integer>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(Integer value) {
                fail("Unexpected transaction result");
            }

            @Override
            public void onError(Throwable t) {
                assertSame(beginFailure, t);
                events.add("onError");
            }

            @Override
            public void onComplete() {
                fail("A failed BEGIN completed the transaction");
            }
        });

        assertEquals(List.of("beginTransaction", "close"), events);
        assertNotNull(close.get());

        close.get().onComplete();

        assertEquals(List.of("beginTransaction", "close", "onError"), new ArrayList<>(events));
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
            throw new AssertionError("BEGIN did not fail");
    }
}
