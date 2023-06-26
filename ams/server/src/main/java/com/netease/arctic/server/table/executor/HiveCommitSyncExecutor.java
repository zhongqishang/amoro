package com.netease.arctic.server.table.executor;

import com.netease.arctic.ams.api.TableFormat;
import com.netease.arctic.hive.HiveTableProperties;
import com.netease.arctic.hive.table.SupportHive;
import com.netease.arctic.hive.utils.CompatibleHivePropertyUtil;
import com.netease.arctic.hive.utils.HivePartitionUtil;
import com.netease.arctic.server.table.ServerTableIdentifier;
import com.netease.arctic.server.table.TableManager;
import com.netease.arctic.server.table.TableRuntime;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.UnkeyedTable;
import com.netease.arctic.utils.TablePropertyUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class HiveCommitSyncExecutor extends BaseTableExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCommitSyncExecutor.class);

  // 10 minutes
  private static final long INTERVAL = 10 * 60 * 1000L;

  public HiveCommitSyncExecutor(TableManager tableRuntimes, int poolSize) {
    super(tableRuntimes, poolSize);
  }

  @Override
  protected long getNextExecutingTime(TableRuntime tableRuntime) {
    return INTERVAL;
  }

  @Override
  protected boolean enabled(TableRuntime tableRuntime) {
    return true;
  }

  @Override
  protected void execute(TableRuntime tableRuntime) {
    long startTime = System.currentTimeMillis();
    ServerTableIdentifier tableIdentifier = tableRuntime.getTableIdentifier();
    try {
      ArcticTable arcticTable = loadTable(tableRuntime);
      if (arcticTable.format() != TableFormat.MIXED_HIVE) {
        LOG.debug("{} is not a support hive table", tableIdentifier);
        return;
      }
      LOG.info("{} start hive sync", tableIdentifier);
      syncIcebergToHive(arcticTable);
    } catch (Exception e) {
      LOG.error("{} hive sync failed", tableIdentifier, e);
    } finally {
      LOG.info("{} hive sync finished, cost {}ms", tableIdentifier,
          System.currentTimeMillis() - startTime);
    }
  }

  public static void syncIcebergToHive(ArcticTable arcticTable) throws Exception {
    UnkeyedTable baseTable = arcticTable.isKeyedTable() ?
        arcticTable.asKeyedTable().baseTable() : arcticTable.asUnkeyedTable();
    StructLikeMap<Map<String, String>> partitionProperty = baseTable.partitionProperty();

    if (arcticTable.spec().isUnpartitioned()) {
      syncNoPartitionTable(arcticTable, partitionProperty);
    } else {
      syncPartitionTable(arcticTable, partitionProperty);
    }
  }

  /**
   * once getRuntime location from iceberg property, should update hive table location,
   * because only arctic update hive table location for unPartitioned table.
   */
  private static void syncNoPartitionTable(
      ArcticTable arcticTable,
      StructLikeMap<Map<String, String>> partitionProperty) {
    Map<String, String> property = partitionProperty.get(TablePropertyUtil.EMPTY_STRUCT);
    if (property == null || property.get(HiveTableProperties.PARTITION_PROPERTIES_KEY_HIVE_LOCATION) == null) {
      LOG.debug("{} has no hive location in partition property", arcticTable.id());
      return;
    }

    String currentLocation = property.get(HiveTableProperties.PARTITION_PROPERTIES_KEY_HIVE_LOCATION);
    String hiveLocation;
    try {
      hiveLocation = ((SupportHive) arcticTable).getHMSClient().run(client -> {
        Table hiveTable = client.getTable(arcticTable.id().getDatabase(), arcticTable.id().getTableName());
        return hiveTable.getSd().getLocation();
      });
    } catch (Exception e) {
      LOG.error("{} getRuntime hive location failed", arcticTable.id(), e);
      return;
    }

    if (!Objects.equals(currentLocation, hiveLocation)) {
      try {
        ((SupportHive) arcticTable).getHMSClient().run(client -> {
          Table hiveTable = client.getTable(arcticTable.id().getDatabase(), arcticTable.id().getTableName());
          hiveTable.getSd().setLocation(currentLocation);
          client.alterTable(arcticTable.id().getDatabase(), arcticTable.id().getTableName(), hiveTable);
          return null;
        });
      } catch (Exception e) {
        LOG.error("{} alter hive location failed", arcticTable.id(), e);
      }
    }
  }

  private static void syncPartitionTable(
      ArcticTable arcticTable,
      StructLikeMap<Map<String, String>> partitionProperty) throws Exception {
    Map<String, StructLike> icebergPartitionMap = new HashMap<>();
    for (StructLike structLike : partitionProperty.keySet()) {
      icebergPartitionMap.put(arcticTable.spec().partitionToPath(structLike), structLike);
    }
    List<String> icebergPartitions = new ArrayList<>(icebergPartitionMap.keySet());
    List<Partition> hivePartitions = ((SupportHive) arcticTable).getHMSClient().run(client ->
        client.listPartitions(arcticTable.id().getDatabase(), arcticTable.id().getTableName(), Short.MAX_VALUE));
    List<String> hivePartitionNames = ((SupportHive) arcticTable).getHMSClient().run(client ->
        client.listPartitionNames(arcticTable.id().getDatabase(), arcticTable.id().getTableName(), Short.MAX_VALUE));
    List<FieldSchema> partitionKeys = ((SupportHive) arcticTable).getHMSClient().run(client -> {
      Table hiveTable = client.getTable(arcticTable.id().getDatabase(), arcticTable.id().getTableName());
      return hiveTable.getPartitionKeys();
    });
    Map<String, Partition> hivePartitionMap = new HashMap<>();
    for (Partition hivePartition : hivePartitions) {
      hivePartitionMap.put(Warehouse.makePartName(partitionKeys, hivePartition.getValues()), hivePartition);
    }

    Set<String> inIcebergNotInHive = icebergPartitions.stream()
        .filter(partition -> !hivePartitionNames.contains(partition))
        .collect(Collectors.toSet());
    Set<String> inHiveNotInIceberg = hivePartitionNames.stream()
        .filter(partition -> !icebergPartitions.contains(partition))
        .collect(Collectors.toSet());
    Set<String> inBoth = icebergPartitions.stream()
        .filter(hivePartitionNames::contains)
        .collect(Collectors.toSet());

    if (CollectionUtils.isNotEmpty(inIcebergNotInHive)) {
      handleInIcebergPartitions(arcticTable, inIcebergNotInHive, icebergPartitionMap, partitionProperty);
    }

    if (CollectionUtils.isNotEmpty(inHiveNotInIceberg)) {
      handleInHivePartitions(arcticTable, inHiveNotInIceberg, hivePartitionMap);
    }

    if (CollectionUtils.isNotEmpty(inBoth)) {
      handleInBothPartitions(arcticTable, inBoth, hivePartitionMap, icebergPartitionMap, partitionProperty);
    }
  }

  /**
   * if iceberg partition location is existed, should update hive table location.
   */
  private static void handleInIcebergPartitions(
      ArcticTable arcticTable,
      Set<String> inIcebergNotInHive,
      Map<String, StructLike> icebergPartitionMap,
      StructLikeMap<Map<String, String>> partitionProperty) {
    inIcebergNotInHive.forEach(partition -> {
      Map<String, String> property = partitionProperty.get(icebergPartitionMap.get(partition));
      if (property == null || property.get(HiveTableProperties.PARTITION_PROPERTIES_KEY_HIVE_LOCATION) == null) {
        return;
      }
      String currentLocation = property.get(HiveTableProperties.PARTITION_PROPERTIES_KEY_HIVE_LOCATION);

      if (arcticTable.io().exists(currentLocation)) {
        int transientTime = Integer.parseInt(property
            .getOrDefault(HiveTableProperties.PARTITION_PROPERTIES_KEY_TRANSIENT_TIME, "0"));
        List<DataFile> dataFiles = getIcebergPartitionFiles(arcticTable, icebergPartitionMap.get(partition));
        HivePartitionUtil.createPartitionIfAbsent(((SupportHive) arcticTable).getHMSClient(),
            arcticTable,
            HivePartitionUtil.partitionValuesAsList(
                icebergPartitionMap.get(partition),
                arcticTable.spec().partitionType()),
            currentLocation, dataFiles, transientTime);
      }
    });
  }

  private static void handleInHivePartitions(
      ArcticTable arcticTable,
      Set<String> inHiveNotInIceberg,
      Map<String, Partition> hivePartitionMap) {
    inHiveNotInIceberg.forEach(partition -> {
      Partition hivePartition = hivePartitionMap.get(partition);
      boolean isArctic = CompatibleHivePropertyUtil.propertyAsBoolean(hivePartition.getParameters(),
          HiveTableProperties.ARCTIC_TABLE_FLAG, false);
      if (isArctic) {
        HivePartitionUtil.dropPartition(((SupportHive) arcticTable).getHMSClient(), arcticTable, hivePartition);
      }
    });
  }

  private static void handleInBothPartitions(
      ArcticTable arcticTable,
      Set<String> inBoth,
      Map<String, Partition> hivePartitionMap,
      Map<String, StructLike> icebergPartitionMap,
      StructLikeMap<Map<String, String>> partitionProperty) {
    Set<String> inHiveNotInIceberg = new HashSet<>();
    inBoth.forEach(partition -> {
      Map<String, String> property = partitionProperty.get(icebergPartitionMap.get(partition));
      if (property == null || property.get(HiveTableProperties.PARTITION_PROPERTIES_KEY_HIVE_LOCATION) == null) {
        inHiveNotInIceberg.add(partition);
        return;
      }

      String currentLocation = property.get(HiveTableProperties.PARTITION_PROPERTIES_KEY_HIVE_LOCATION);
      Partition hivePartition = hivePartitionMap.get(partition);

      if (!Objects.equals(currentLocation, hivePartition.getSd().getLocation())) {
        int transientTime = Integer.parseInt(property
            .getOrDefault(HiveTableProperties.PARTITION_PROPERTIES_KEY_TRANSIENT_TIME, "0"));
        List<DataFile> dataFiles = getIcebergPartitionFiles(arcticTable, icebergPartitionMap.get(partition));
        HivePartitionUtil.updatePartitionLocation(((SupportHive) arcticTable).getHMSClient(),
            arcticTable, hivePartition, currentLocation, dataFiles, transientTime);
      }
    });

    handleInHivePartitions(arcticTable, inHiveNotInIceberg, hivePartitionMap);
  }

  private static List<DataFile> getIcebergPartitionFiles(
      ArcticTable arcticTable,
      StructLike partition) {
    UnkeyedTable baseStore;
    baseStore = arcticTable.isKeyedTable() ? arcticTable.asKeyedTable().baseTable() : arcticTable.asUnkeyedTable();

    List<DataFile> partitionFiles = new ArrayList<>();
    arcticTable.io().doAs(() -> {
      try (CloseableIterable<FileScanTask> fileScanTasks = baseStore.newScan().planFiles()) {
        for (FileScanTask fileScanTask : fileScanTasks) {
          if (fileScanTask.file().partition().equals(partition)) {
            partitionFiles.add(fileScanTask.file());
          }
        }
      }

      return null;
    });

    return partitionFiles;
  }
}
