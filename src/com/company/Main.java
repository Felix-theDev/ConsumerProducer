package com.company;

/**The Producer-Consumer problem is a classic problem this is used for
     multi-process synchronization i.e. synchronization between more than one processes.
    In the producer-consumer problem, there is one Producer that is producing something and there is one Consumer
    that is consuming the products produced by the Producer.
 * @author Felix Ogbonnaya
 * @since 2020-02-17
 */

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Main {
    private static Buffer buffer = new Buffer();

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.execute(new ProducerTask());
        executor.execute(new ConsumerTask());
    }
    private static class ProducerTask implements Runnable{
        public void run(){
            try{
                int i =1;
                while(true){
                    System.out.println("Producer writes "+ i);
                    buffer.write(i++);
                    Thread.sleep((int)(Math.random() * 10000));

                }

            }
            catch(InterruptedException ex){
                ex.printStackTrace();
            }
        }
    }
    private static class ConsumerTask implements Runnable{
        public void run(){
            try{
                while (true){
                    System.out.println("\t\t\tConsumer reads "+ buffer.read());

                    Thread.sleep((int)(Math.random() * 10000));
                }
            }catch (InterruptedException ex){
                ex.printStackTrace();
            }
        }
    }


    private static class Buffer{
        private static final int CAPACITY = 1;
        LinkedList queue = new LinkedList();

        private static Lock lock = new ReentrantLock();

        private static Condition notEmpty = lock.newCondition();
        private static Condition notFull = lock.newCondition();

        public void write(int value){
            lock.lock();
            try{
                while(queue.size() == CAPACITY){
                    System.out.println("Wait for not full condition");
                    notFull.await();
                }
                queue.offer(value);
                notEmpty.signal();
            }
            catch (InterruptedException ex){
                ex.printStackTrace();
            }
            finally {
                lock.unlock();
            }

        }
        public int read() {
            int value = 0;
            lock.lock();
            try {
                while (queue.isEmpty()) {
                    System.out.println("Wait for non empty condition");
                    notEmpty.await();
                }
                value = (int)queue.remove();
                notFull.signal();
            }

            catch (InterruptedException ex){
                ex.printStackTrace();
            }
            finally {
                lock.unlock();
                return value;
            }
            }

    }
}
