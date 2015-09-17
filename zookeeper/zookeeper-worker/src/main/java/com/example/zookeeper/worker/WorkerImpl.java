package com.example.zookeeper.worker;

import static com.example.zookeeper.Constants.DATA_NODE;
import static com.example.zookeeper.Constants.LOCK_NODE;
import static java.lang.Thread.currentThread;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.osgi.framework.Constants.FRAMEWORK_UUID;
import static org.osgi.service.component.annotations.ConfigurationPolicy.REQUIRE;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.zookeeper.Broadcast;

@Component(configurationPid="com.example.zookeeper.worker", configurationPolicy=REQUIRE)
public class WorkerImpl {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerImpl.class);
    
    @Reference
    Broadcast broadcast;
    
    String id;
    
    CuratorFramework client;

    InterProcessMutex lock;
    
    Thread worker;

    @ObjectClassDefinition
    @interface Config {
        @AttributeDefinition(name="Zookeeper connection")
        String zk_connection();
    }
    
    @Activate
    void start(BundleContext context, Config config) {
        id = context.getProperty(FRAMEWORK_UUID).substring(0, 8);

        LOGGER.info("Starting the ZooKeeper Worker component with id {}", id);
        
        client = newClient(config.zk_connection(), 
                new ExponentialBackoffRetry(1000, 3));
        client.start();
        
        lock = new InterProcessMutex(client, LOCK_NODE);
        
        worker = new Thread(this::work);
        
        worker.start();
        
        LOGGER.info("The ZooKeeper Worker component with id {} started successfully", id);
    }


    @Deactivate
    void stop() {
        LOGGER.info("Stopping the ZooKeeper Worker component with id {}", id);
        worker.interrupt();
        try {
            worker.join(3000);
        } catch (InterruptedException e) { 
            currentThread().interrupt();
        }
        client.close();
    }


    private void work() {
        for(;;) {
            try {
                Thread.sleep(2000);
                
                LOGGER.debug("Attempting to aquire the distributed lock for worker {}", id);
                lock.acquire();
                try {
                    LOGGER.debug("Aquired the distributed lock for worker {}", id);
                    adjustNode();
                } finally {
                    LOGGER.debug("Releasing the distributed lock for worker {}", id);
                    lock.release();
                }
            } catch (InterruptedException ie) { 
                LOGGER.debug("Shutting down worker {}", id);
                broadcast.notifyUsers("Framework " + id +
                        " is shutting down");
                break;
            } catch (Exception e) {
                LOGGER.warn("An error occurred in worker worker {}", id);
                broadcast.notifyUsers("An error occured in Framework " + id +
                        ": " + e.getMessage());
            }
        }
    }
    
    private void adjustNode() throws Exception {
        Stat s = client.checkExists()
                .forPath(DATA_NODE);
            
            if(s == null) {
                // No node to process
                LOGGER.debug("The worker {} has nothing to do", id);
                return;
            }

            broadcast.notifyUsers("Framework " + 
                    id + " acquired the lock!");
            
            Thread.sleep(2000);
            
            Stat stat = new Stat();
            
            byte[] data = client.getData()
                .storingStatIn(stat)
                .forPath(DATA_NODE);
            
            byte newByte = data[0];
            newByte--;
            
            if(newByte <= 0) {
                client.delete()
                    .withVersion(stat.getVersion())
                    .forPath(DATA_NODE);
                LOGGER.debug("The worker {} has finished the countdown", id);
                
                broadcast.notifyUsers("Framework " + 
                        id + " finshed counting down");
            } else {
                client.setData()
                    .withVersion(stat.getVersion())
                    .forPath(DATA_NODE, 
                            new byte[] {newByte});
                LOGGER.debug("The worker {} has advanced the countdown to {}", id, newByte);
                
                broadcast.notifyUsers("Framework " + 
                        id + " counted down from " + data[0]
                                + " to " + newByte);
            }
            
            Thread.sleep(2000);
            
            broadcast.notifyUsers("Framework " + 
                    id + " releasing the lock!");
    }
}
