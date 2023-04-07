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

package com.netease.arctic.ams.server.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.request.OapiRobotSendRequest;
import com.dingtalk.api.response.OapiRobotSendResponse;
import com.netease.arctic.ams.server.config.ConfigFileProperties;
import com.netease.arctic.ams.server.service.AlertService;
import org.apache.commons.net.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

public class DingTalkAlertService implements AlertService {

  private static final Logger LOG = LoggerFactory.getLogger(DingTalkAlertService.class);
  public DefaultDingTalkClient dingTalkClient;
  private boolean enable = false;
  private boolean secret = false;
  private String alertUrl;
  private String token;
  private String secretToken;
  private int pendingThreshold;

  public DingTalkAlertService() {
  }

  @Override
  public void alert(String msg) {
    if (!enable) {
      return;
    }

    String dingUrl = String.format("%s?access_token=%s", alertUrl, token);
    try {
      if (secret) {
        Long timestamp = System.currentTimeMillis();
        dingUrl = String.format("%s&timestamp=%d&sign=%s", dingUrl, timestamp, getSign(secretToken, timestamp));
      }
      dingTalkClient = new DefaultDingTalkClient(dingUrl);

      OapiRobotSendRequest request = new OapiRobotSendRequest();
      request.setMsgtype("markdown");
      OapiRobotSendRequest.Markdown markdown = new OapiRobotSendRequest.Markdown();
      markdown.setTitle("AMS 任务预警");
      String sb = "## AMS 告警" + "\n" +
          "### " + msg + "\n" +
          "Timestamp : " + LocalDateTime.now() +
          "\n";
      markdown.setText(sb);
      request.setMarkdown(markdown);
      OapiRobotSendRequest.At at = new OapiRobotSendRequest.At();
      at.setIsAtAll(false);
      request.setAt(at);
      OapiRobotSendResponse response;
      response = dingTalkClient.execute(request);
      LOG.info("Alert success : {}", markdown);
      if (!response.isSuccess()) {
        LOG.error("Push DingTalk Error. {} : {}", response.getErrcode(), response.getErrmsg());
      }
    } catch (Exception e) {
      LOG.error("Push DingTalk Error.", e);
    }
  }

  @Override
  public void init(JSONObject alertConfig) {
    alertUrl = alertConfig.getString(ConfigFileProperties.ALERT_URL);
    token = alertConfig.getString(ConfigFileProperties.ALERT_TOKEN);
    secret = alertConfig.getBoolean(ConfigFileProperties.ALERT_SECRET_ENABLE);
    secretToken = alertConfig.getString(ConfigFileProperties.ALERT_SECRET_TOKEN);
    enable = alertConfig.getBoolean(ConfigFileProperties.ALERT_ENABLE);
    pendingThreshold = alertConfig.getInteger(ConfigFileProperties.ALERT_PENDING_TABLE_THRESHOLD);
  }

  @Override
  public int getPendingTablesThreshold() {
    return pendingThreshold;
  }

  private String getSign(String secret, Long timestamp) throws Exception {
    try {
      String stringToSign = timestamp + "\n" + secret;
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
      byte[] signData = mac.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8));
      return URLEncoder.encode(new String(Base64.encodeBase64(signData)), "UTF-8");
    } catch (Exception e) {
      throw new Exception("Calculate the signature failed.", e);
    }
  }
}
