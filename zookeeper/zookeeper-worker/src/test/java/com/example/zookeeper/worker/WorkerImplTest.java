package com.example.zookeeper.worker;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;
import static org.osgi.framework.Constants.FRAMEWORK_UUID;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;

import com.example.zookeeper.Broadcast;
import com.example.zookeeper.worker.WorkerImpl.Config;


@RunWith(MockitoJUnitRunner.class)
public class WorkerImplTest {
    
    @Mock
    BundleContext ctx;

    @Mock
    Config config;
    
    TestingServer server;
    
    CuratorFramework client;

    WorkerImpl workerImpl;

    BlockingQueue<String> messages = new LinkedBlockingQueue<>();
    
    @Before
    public void setup() throws Exception {
        server = new TestingServer();
        
        client = newClient(server.getConnectString(), 
                new ExponentialBackoffRetry(1000, 3));
        client.start();

        when(ctx.getProperty(FRAMEWORK_UUID)).thenReturn("abcdefgh");
        when(config.zk_connection()).thenReturn(server.getConnectString());
        
        Broadcast b = messages::add;
        
        workerImpl = new WorkerImpl();
        workerImpl.broadcast = b;
        
        workerImpl.start(ctx, config);
    }
    
    @After
    public void tearDown() throws Exception {
        if(workerImpl == null) {
            workerImpl.stop();
        }
        
        client.close();
        server.close();
    }
    
    @Test
    public void testCountdown() throws Exception {
        
        assertNull(messages.poll(5, SECONDS));
        
        client.create()
            .creatingParentsIfNeeded()
            .forPath("/com/example/zookeeper/locking/data", 
                    new byte[] {2});
        
        
        assertEquals("Framework abcdefgh acquired the lock!",
                messages.poll(5, SECONDS));
        assertEquals("Framework abcdefgh counted down from 2 to 1",
                messages.poll(5, SECONDS));
        assertEquals("Framework abcdefgh releasing the lock!",
                messages.poll(5, SECONDS));

        assertEquals("Framework abcdefgh acquired the lock!",
                messages.poll(5, SECONDS));
        assertEquals("Framework abcdefgh finshed counting down",
                messages.poll(5, SECONDS));
        assertEquals("Framework abcdefgh releasing the lock!",
                messages.poll(5, SECONDS));

        assertNull(messages.poll(5, SECONDS));
        
        assertNull(client.checkExists()
                    .forPath("/com/example/zookeeper/locking/data"));
    }
}
