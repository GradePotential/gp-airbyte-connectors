/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mongodb.cdc;

import io.airbyte.integrations.source.mongodb.MongoConstants;
import java.time.Duration;
import java.util.Properties;

/**
 * Defines MongoDB specific CDC configuration properties for Debezium.
 */
public class MongoDbCdcProperties {

  static final String CAPTURE_MODE_KEY = "capture.mode";
  static final String CAPTURE_MODE_VALUE_LOOKUP = "change_streams_update_full";
  static final String CAPTURE_MODE_VALUE_POST_IMAGE = "change_streams_update_full_with_pre_image";
  static final String CONNECTOR_CLASS_KEY = "connector.class";
  static final String CONNECTOR_CLASS_VALUE = "io.debezium.connector.mongodb.MongoDbConnector";
  static final String HEARTBEAT_FREQUENCY_MS = Long.toString(Duration.ofSeconds(10).toMillis());
  static final String HEARTBEAT_INTERVAL_KEY = "heartbeat.interval.ms";
  static final String SNAPSHOT_MODE_KEY = "snapshot.mode";
  static final String SNAPSHOT_MODE_VALUE = "never";
  static final String TOMBSTONE_ON_DELETE_KEY = "tombstones.on.delete";
  static final String TOMBSTONE_ON_DELETE_VALUE = Boolean.FALSE.toString();

  /**
   * Returns the common properties required to configure the Debezium MongoDB connector.
   *
   * @return The common Debezium CDC properties for the Debezium MongoDB connector.
   */
  public static Properties getDebeziumProperties(final String updateCaptureMode) {
    final Properties props = new Properties();

    final String captureMode = MongoConstants.CAPTURE_MODE_POST_IMAGE_OPTION.equals(updateCaptureMode)
        ? CAPTURE_MODE_VALUE_POST_IMAGE
        : CAPTURE_MODE_VALUE_LOOKUP;

    props.setProperty(CONNECTOR_CLASS_KEY, CONNECTOR_CLASS_VALUE);
    props.setProperty(SNAPSHOT_MODE_KEY, SNAPSHOT_MODE_VALUE);
    props.setProperty(CAPTURE_MODE_KEY, captureMode);
    props.setProperty(HEARTBEAT_INTERVAL_KEY, HEARTBEAT_FREQUENCY_MS);
    props.setProperty(TOMBSTONE_ON_DELETE_KEY, TOMBSTONE_ON_DELETE_VALUE);

    return props;
  }

}
