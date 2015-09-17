package com.example.zookeeper.web;

import static com.example.zookeeper.Constants.DATA_NODE;
import static com.example.zookeeper.Constants.LOCK_NODE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.eclipse.jetty.websocket.api.StatusCode.SHUTDOWN;
import static org.osgi.service.component.annotations.ConfigurationPolicy.REQUIRE;
import static org.osgi.service.http.whiteboard.HttpWhiteboardConstants.HTTP_WHITEBOARD_RESOURCE_PATTERN;
import static org.osgi.service.http.whiteboard.HttpWhiteboardConstants.HTTP_WHITEBOARD_RESOURCE_PREFIX;
import static org.osgi.service.http.whiteboard.HttpWhiteboardConstants.HTTP_WHITEBOARD_SERVLET_PATTERN;
import static org.osgi.service.remoteserviceadmin.RemoteConstants.SERVICE_EXPORTED_INTERFACES;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.zookeeper.Broadcast;
import com.paremus.http.publish.HttpEndpointPublisher;

@Component(property = { HTTP_WHITEBOARD_SERVLET_PATTERN + "=/zookeeper/listen",
        HTTP_WHITEBOARD_RESOURCE_PATTERN + "=/zookeeper/static/*",
        HTTP_WHITEBOARD_RESOURCE_PREFIX + "=/static", 
        SERVICE_EXPORTED_INTERFACES + "=com.example.zookeeper.Broadcast",
        HttpEndpointPublisher.APP_ENDPOINT + "=/zookeeper/static/index.html"},
   configurationPid = "com.example.zookeeper.web", configurationPolicy=REQUIRE)
public class ZooKeeperServlet extends WebSocketServlet implements Servlet, Broadcast {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperServlet.class);
    
    private static final long serialVersionUID = 1L;
    
    private Set<Notification> toNotify = new CopyOnWriteArraySet<>();

    private CuratorFramework client;
    
    private InterProcessMutex lock;
    
    private final AtomicBoolean firstCall = new AtomicBoolean(true);
    
    private final CountDownLatch initBarrier = new CountDownLatch(1);

    @ObjectClassDefinition
    @interface Config {
        @AttributeDefinition(name="Zookeeper connection")
        String zk_connection();
    }
    
    @Activate
    void start(Config config) {
        LOGGER.info("Starting the ZooKeeper Web component");
        
        client = newClient(config.zk_connection(), 
                new ExponentialBackoffRetry(1000, 3));
        
        client.start();
        
        lock = new InterProcessMutex(client, LOCK_NODE);
        
        LOGGER.info("The ZooKeeper Web component started successfully");
    }
    
    @Deactivate
    void stop() {
        LOGGER.info("Stopping the ZooKeeper Web component");

        toNotify.stream()
            .filter(Notification::isConnected)
            .map(Notification::getSession)
            .forEach(s -> s.close(SHUTDOWN, "Closing"));
    }
    
    @Override
    public void init() throws ServletException {
      LOGGER.info("The ZooKeeper servlet has been initialized, but we delay initialization until the first request so that a Jetty Context is available");    
    }
    
    @Override
    public void service(ServletRequest arg0, ServletResponse arg1) throws ServletException, IOException {
        if(firstCall.compareAndSet(true, false)) {
            try {
                delayedInit();
            } finally {
                initBarrier.countDown();
            }
        } else {
            try {
                initBarrier.await();
            } catch (InterruptedException e) {
                throw new ServletException("Timed out waiting for initialisation", e);
            }
        }
        
        super.service(arg0, arg1);
    }

    private void delayedInit() throws ServletException {
        // Overide the TCCL so that the internal factory can be found
        // Jetty tries to use ServiceLoader, and their fallback is to
        // use TCCL, it would be better if we could provide a loader...
        
        Thread currentThread = Thread.currentThread();
        ClassLoader tccl = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(WebSocketServlet.class.getClassLoader());
        try {
            super.init();
        } finally {
            currentThread.setContextClassLoader(tccl);
        }
    }

    @Override
    public void configure(WebSocketServletFactory wsf) {
        wsf.setCreator((req,res) -> new Notification());
    }

    @Override
    public void notifyUsers(String message) {
        LOGGER.debug("Stopping the ZooKeeper Web component");

        toNotify.stream()
            .filter(Notification::isConnected)
            .map(Notification::getRemote)
            .forEach(r -> r.sendStringByFuture(message));
    }

    private class Notification extends WebSocketAdapter {

        @Override
        public void onWebSocketText(String message) {
            LOGGER.debug("A Web Socket client with session {} sent a request {}", getSession(), message);
            try {
                byte b = Byte.parseByte(message);
                
                if(lock.acquire(10, SECONDS)) {
                    try {
                        setOrUpdate(b);
                    } finally {
                        lock.release();
                    }
                } else {
                    getRemote().sendStringByFuture("Timed out waiting to set the data node");
                }
            } catch (Exception e) {
                getRemote().sendStringByFuture("An error occurred: " + e.getMessage());
            }
            
        }

        private void setOrUpdate(byte b) throws Exception {
            Stat stat = client.checkExists()
                .forPath(DATA_NODE);
            
            if(stat == null) {
                client.create()
                    .creatingParentsIfNeeded()
                    .forPath(DATA_NODE, new byte[] {b});
            } else {
                client.setData()
                    .withVersion(stat.getVersion())
                    .forPath(DATA_NODE, new byte[] {b});
            }
        }

        @Override
        public void onWebSocketConnect(Session sess) {
            LOGGER.debug("A new Web Socket client connected with session {}", sess);
            super.onWebSocketConnect(sess);
            toNotify.add(this);
        }

        @Override
        public void onWebSocketError(Throwable t) {
            LOGGER.warn("The Web Socket client with session {} failed", getSession(), t);
            toNotify.remove(this);
            super.onWebSocketError(t);
        }
        
        @Override
        public void onWebSocketClose(int statusCode, String reason) {
            LOGGER.debug("The Web Socket client with session {} closed", getSession());
            toNotify.remove(this);
            super.onWebSocketClose(statusCode, reason);
        }
    }
}
