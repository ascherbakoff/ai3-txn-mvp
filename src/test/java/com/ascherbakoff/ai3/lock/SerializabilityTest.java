package com.ascherbakoff.ai3.lock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;

public class SerializabilityTest {
    private static final Logger LOG = System.getLogger(SerializabilityTest.class.getName());

    Map<Integer, Integer> accounts = new HashMap<>();

    /**
     * Tests if a locking provides serializability.
     */
    @Test
    public void testBalanceWaitDie() throws InterruptedException {
        //doTestSingleKeyMultithreaded(Runtime.getRuntime().availableProcessors() * 2, 5_000, false);
        doTestSingleKeyMultithreaded(2, 5_000, new LockTable(10, true, DeadlockPrevention.waitDie()));
    }

    /**
     * Performs a test.
     *
     * @param threadsCnt Thread count.
     * @param duration The duration.
     * @param lockTable Lock table.
     * @throws InterruptedException If interrupted while waiting.
     */
    private void doTestSingleKeyMultithreaded(int threadsCnt, long duration, LockTable lockTable) throws InterruptedException {
        Thread[] threads = new Thread[threadsCnt];

        final int initial = 1000;
        final int total = threads.length * initial;

        for (int i = 0; i < threads.length; i++) {
            accounts.put(i, 1000);
        }

        int total0 = 0;

        for (int i = 0; i < threads.length; i++) {
            int balance = accounts.get(i);

            total0 += balance;
        }

        assertEquals(total, total0, "Total amount invariant is not preserved");

        CyclicBarrier startBar = new CyclicBarrier(threads.length, () -> LOG.log(Level.INFO, "Before test"));

        LongAdder ops = new LongAdder();
        LongAdder fails = new LongAdder();

        AtomicBoolean stop = new AtomicBoolean();

        Random r = new Random();

        AtomicLong ts = new AtomicLong();
        AtomicReference<Throwable> firstErr = new AtomicReference<>();

        for (int i = 0; i < threads.length; i++) {
            int finalI = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        startBar.await();
                    } catch (Exception e) {
                        fail();
                    }

                    while (!stop.get() && firstErr.get() == null) {
                        UUID id = new UUID(0, ts.getAndIncrement()); // Id is kept on retry.

                        while (!stop.get() && firstErr.get() == null) { // Retry loop.
                            int acc1 = finalI;

                            int acc2 = r.nextInt(threads.length);

                            while (acc1 == acc2) {
                                acc2 = r.nextInt(threads.length);
                            }

                            Locker l1 = null;
                            Locker l2 = null;

                            Lock lock1 = lockTable.getOrAddEntry(acc1);
                            Lock lock2 = lockTable.getOrAddEntry(acc2);

                            try {
                                int amount = 100 + r.nextInt(500);

                                // Get S_lock(acc1)
                                l1 = lock1.acquire(id, LockMode.S);
                                l1.join();

                                // Get S_lock(acc2)
                                l2 = lock2.acquire(id, LockMode.S);
                                l2.join();

                                int val0 = accounts.get(acc1);
                                int val1 = accounts.get(acc2);

                                lock1.acquire(id, LockMode.X).join();
                                lock2.acquire(id, LockMode.X).join();

                                accounts.put(acc1, val0 - amount);
                                accounts.put(acc2, val1 + amount);

                                ops.increment();

                                break;
                            } catch (LockException e) {
                                fails.increment();

                                continue; // Retry with the same id.
                            }
                            finally {
                                if (l1 != null) {
                                    lock1.release(l1);
                                }

                                if (l2 != null) {
                                    lock2.release(l2);
                                }
                            }
                        }
                    }
                }
            });

            threads[i].setName("Worker-" + i);
            threads[i].setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    firstErr.compareAndExchange(null, e);
                }
            });
            threads[i].start();
        }

        Thread.sleep(duration);

        stop.set(true);

        for (Thread thread : threads) {
            thread.join(3_000);
        }

        if (firstErr.get() != null) {
            throw new RuntimeException(firstErr.get());
        }

        LOG.log(Level.INFO, "After test ops=" + ops.sum() + ", fails=" + fails.sum());

        total0 = 0;

        for (int i = 0; i < threads.length; i++) {
            int balance = accounts.get(i);

            total0 += balance;
        }

        assertEquals(total, total0, "Total amount invariant is not preserved");
    }
}
