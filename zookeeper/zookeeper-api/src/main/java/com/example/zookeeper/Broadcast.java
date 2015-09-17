package com.example.zookeeper;

import org.osgi.annotation.versioning.ProviderType;

@ProviderType
public interface Broadcast {
    public void notifyUsers(String message);
}
