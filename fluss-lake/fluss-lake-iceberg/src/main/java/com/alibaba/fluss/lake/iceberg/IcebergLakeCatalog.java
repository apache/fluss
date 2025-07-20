package com.alibaba.fluss.lake.iceberg;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.lake.lakestorage.LakeCatalog;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** A Iceberg implementation of {@link LakeCatalog}. */
public class IcebergLakeCatalog implements LakeCatalog {

  private static final LinkedHashMap<String, Type> SYSTEM_COLUMNS = new LinkedHashMap<>();

  static {
    // We need __bucket system column to filter out the given bucket
    // for iceberg bucket append only table & primary key table.
    SYSTEM_COLUMNS.put(BUCKET_COLUMN_NAME, Types.IntegerType.get());
    SYSTEM_COLUMNS.put(OFFSET_COLUMN_NAME, Types.LongType.get());
    SYSTEM_COLUMNS.put(TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone());
  }

  private final Catalog icebergCatalog;

  // for fluss config
  private static final String FLUSS_CONF_PREFIX = "fluss.";
  // for iceberg config
  private static final String ICEBERG_CONF_PREFIX = "iceberg.";

  public IcebergLakeCatalog(Configuration configuration) {
    Map<String, String> icebergConf = stripPrefix(configuration.toMap(), ICEBERG_CONF_PREFIX);
    this.icebergCatalog =
        CatalogUtil.loadCatalog(
            "org.apache.iceberg.hive.HiveCatalog",
            "hive-iceberg",
            icebergConf,
            new org.apache.hadoop.conf.Configuration());
  }

  @Override
  public void createTable(TablePath tablePath, TableDescriptor tableDescriptor)
      throws TableAlreadyExistException {}

  @Override
  public void close() throws Exception {
    LakeCatalog.super.close();
  }

  private Map<String, String> stripPrefix(Map<String, String> conf, String prefix) {
    return conf.entrySet().stream()
        .filter(e -> e.getKey().startsWith(prefix))
        .collect(Collectors.toMap(e -> e.getKey().substring(prefix.length()), Map.Entry::getValue));
  }
}
