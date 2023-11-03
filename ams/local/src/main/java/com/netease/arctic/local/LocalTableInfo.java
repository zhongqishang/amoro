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

package com.netease.arctic.local;

import com.netease.arctic.AmoroTable;
import com.netease.arctic.ams.api.TableFormat;
import com.netease.arctic.server.ArcticServiceContainer;
import com.netease.arctic.server.optimizing.plan.OptimizingPlanner;
import com.netease.arctic.server.optimizing.plan.TaskDescriptor;
import com.netease.arctic.server.persistence.mapper.TableMetaMapper;
import com.netease.arctic.server.table.DefaultTableService;
import com.netease.arctic.server.table.ServerTableIdentifier;
import com.netease.arctic.server.table.TableRuntime;
import com.netease.arctic.server.table.TableRuntimeMeta;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableIdentifier;
import java.util.List;
import java.util.Optional;

public class LocalTableInfo {

  public static void main(String[] args) throws Exception {

    ArcticServiceContainer service = new ArcticServiceContainer();

    DefaultTableService tableService = new DefaultTableService(service.serviceConfig);
    tableService.initialize();
    // List<CatalogMeta> catalogMetaList = tableService.listCatalogMetas();
    List<TableRuntimeMeta> tableRuntimeMetaList =
        tableService.getAs(TableMetaMapper.class, TableMetaMapper::selectTableRuntimeMetas);

    Optional<TableRuntimeMeta> first =
        tableRuntimeMetaList.stream()
            .filter(tableRuntimeMeta -> tableRuntimeMeta.getTableId() == 1355)
            .findFirst();

    if (!first.isPresent()) {
      return;
    }
    TableRuntimeMeta tableRuntimeMeta = first.get();
    TableRuntime tableRuntime = tableRuntimeMeta.constructTableRuntime(tableService);

    TableIdentifier tableIdentifier =
        TableIdentifier.of("iceberg", "ods_iceberg", "gen_sink_olap_insert");
    AmoroTable<?> amoroTable =
        tableService.loadTable(
            ServerTableIdentifier.of(tableIdentifier.buildTableIdentifier(), TableFormat.ICEBERG));

    OptimizingPlanner planner =
        new OptimizingPlanner(
            tableRuntime.refresh(amoroTable), (ArcticTable) amoroTable.originalTable(), 1.0);
    List<TaskDescriptor> taskDescriptors = planner.planTasks();
    for (TaskDescriptor taskDescriptor : taskDescriptors) {
      System.out.println("ID : " + taskDescriptor.getTableId());
      System.out.println("TaskDescriptor : " + taskDescriptor);
    }
  }
}
