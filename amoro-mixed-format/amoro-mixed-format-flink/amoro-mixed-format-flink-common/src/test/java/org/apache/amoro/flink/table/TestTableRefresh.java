/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.flink.table;

import static org.apache.amoro.flink.table.descriptors.ArcticValidator.LOG_STORE_CATCH_UP;
import static org.apache.amoro.flink.table.descriptors.ArcticValidator.LOG_STORE_CATCH_UP_TIMESTAMP;

import org.apache.amoro.BasicTableTestHelper;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableTestHelper;
import org.apache.amoro.catalog.BasicCatalogTestHelper;
import org.apache.amoro.catalog.CatalogTestHelper;
import org.apache.amoro.flink.FlinkTestBase;
import org.apache.amoro.hive.TestHMS;
import org.apache.amoro.hive.catalog.HiveCatalogTestHelper;
import org.apache.amoro.hive.catalog.HiveTableTestHelper;
import org.apache.amoro.table.ArcticTable;
import org.apache.iceberg.UpdateProperties;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

@RunWith(Parameterized.class)
public class TestTableRefresh extends FlinkTestBase {
  @ClassRule public static TestHMS TEST_HMS = new TestHMS();

  public TestTableRefresh(CatalogTestHelper catalogTestHelper, TableTestHelper tableTestHelper) {
    super(catalogTestHelper, tableTestHelper);
  }

  @Parameterized.Parameters(name = "{0}, {1}")
  public static Collection parameters() {
    return Arrays.asList(
        new Object[][] {
          {
            new HiveCatalogTestHelper(TableFormat.MIXED_HIVE, TEST_HMS.getHiveConf()),
            new HiveTableTestHelper(true, true)
          },
          {
            new BasicCatalogTestHelper(TableFormat.MIXED_ICEBERG),
            new BasicTableTestHelper(true, true)
          }
        });
  }

  @Test
  public void testRefresh() {
    ArcticTableLoader tableLoader =
        ArcticTableLoader.of(TableTestHelper.TEST_TABLE_ID, catalogBuilder);

    tableLoader.open();
    ArcticTable arcticTable = tableLoader.loadArcticTable();
    boolean catchUp = true;
    String catchUpTs = "1";

    UpdateProperties updateProperties = arcticTable.updateProperties();
    updateProperties.set(LOG_STORE_CATCH_UP.key(), String.valueOf(catchUp));
    updateProperties.set(LOG_STORE_CATCH_UP_TIMESTAMP.key(), catchUpTs);
    updateProperties.commit();

    arcticTable.refresh();
    Map<String, String> properties = arcticTable.properties();
    Assert.assertEquals(String.valueOf(catchUp), properties.get(LOG_STORE_CATCH_UP.key()));
    Assert.assertEquals(catchUpTs, properties.get(LOG_STORE_CATCH_UP_TIMESTAMP.key()));
  }
}
