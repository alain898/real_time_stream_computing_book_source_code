package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.ignite;

import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.ConfigHolder;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.ArrayList;
import java.util.List;

public class ApacheIgniteManager {
    private static Ignite ignite = null;

    private static Config config = ConfigHolder.getInstance();

    private static final String cluster = config.getString("ignite.cluster");
    private static final int metaPort = config.getInt("ignite.meta_port");
    private static final int dataPort = config.getInt("ignite.data_port");
    private static final long maxMemoryBytes = config.getLong("ignite.max_memory_bytes");
    private static final boolean persistenceEnabled = config.getBoolean("ignite.persistence_enabled");
    private static final String persistencePath = config.getString("ignite.persistence_path");
    private static final String workPath = String.format("%s/work", persistencePath);
    private static final String walPath = String.format("%s/wal", persistencePath);
    private static final String walArchivePath = String.format("%s/wal_archive", persistencePath);

    private static List<String> parseClusterHosts(String cluster, int metaPort) {
        Preconditions.checkNotNull(cluster, "cluster is null");
        Preconditions.checkArgument(StringUtils.containsNone(":"),
                "cluster[%s] must not contain port number", cluster);
        Preconditions.checkArgument(metaPort > 0,
                "metaPort[%d] must be positive", metaPort);

        String[] splits = cluster.split(",");
        Preconditions.checkArgument(splits.length > 0,
                "invalid cluster[%s]", cluster);

        List<String> hosts = new ArrayList<>();
        for (String split : splits) {
            if (StringUtils.isNotEmpty(split)) {
                hosts.add(String.format("%s:%d", split, metaPort));
            }
        }
        return hosts;
    }


    private static void init() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        // configApp discovery port, used to transport meta data
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setLocalPort(metaPort);
        discoverySpi.setLocalPortRange(0);
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(parseClusterHosts(cluster, metaPort));
        discoverySpi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(discoverySpi);

        // configApp communication port, used to transport data
        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        commSpi.setLocalPort(dataPort);
        cfg.setCommunicationSpi(commSpi);


        // configApp ignite memory
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.getDefaultDataRegionConfiguration().setMaxSize(maxMemoryBytes);
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(persistenceEnabled);
        storageCfg.setStoragePath(workPath);
        storageCfg.setWalPath(walPath);
        storageCfg.setWalArchivePath(walArchivePath);
        cfg.setDataStorageConfiguration(storageCfg);

        ignite = Ignition.start(cfg);
        ignite.active(true);
    }

    public static Ignite instance() {
        if (ignite != null) {
            return ignite;
        }

        synchronized (ApacheIgniteManager.class) {
            if (ignite != null) {
                return ignite;
            }
            init();
            return ignite;
        }
    }
}
