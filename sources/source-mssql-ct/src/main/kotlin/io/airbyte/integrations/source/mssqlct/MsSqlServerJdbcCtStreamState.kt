/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mssqlct

import com.fasterxml.jackson.annotation.JsonProperty
import io.airbyte.cdk.command.OpaqueStateValue
import io.airbyte.cdk.util.Jsons

/**
 * State for a stream in Change Tracking mode.
 *
 * Lifecycle:
 * - **Snapshot in progress**: [pkName] is set (PK column name), [ctVersion] is the version
 *   captured at snapshot start. [pkVal] is the last PK value seen (null = not started yet).
 * - **Incremental (snapshot complete)**: [pkName] is null. [ctVersion] is the last processed
 *   CT version to use as the `sinceVersion` for the next CHANGETABLE query.
 */
data class MsSqlServerJdbcCtStreamState(
    @JsonProperty("ct_version") val ctVersion: Long,
    @JsonProperty("pk_name") val pkName: String? = null,
    @JsonProperty("pk_val") val pkVal: String? = null,
    @JsonProperty("version") val version: Int = 1,
) {
    companion object {
        /**
         * State saved at the start of a snapshot: CT version captured before any rows are read,
         * PK position not yet started.
         */
        fun snapshotStart(ctVersion: Long, pkName: String): OpaqueStateValue =
            Jsons.valueToTree(
                MsSqlServerJdbcCtStreamState(
                    ctVersion = ctVersion,
                    pkName = pkName,
                    pkVal = null,
                )
            )

        /**
         * State saved during snapshot (after reading a batch of rows). Records the last PK value
         * so the snapshot can resume.
         */
        fun snapshotCheckpoint(ctVersion: Long, pkName: String, pkVal: String): OpaqueStateValue =
            Jsons.valueToTree(
                MsSqlServerJdbcCtStreamState(
                    ctVersion = ctVersion,
                    pkName = pkName,
                    pkVal = pkVal,
                )
            )

        /**
         * State saved when snapshot completes. [pkName] = null signals incremental mode. The
         * [ctVersion] here is what was captured at snapshot start — we'll query CHANGETABLE
         * starting from this version.
         */
        fun snapshotComplete(ctVersion: Long): OpaqueStateValue =
            Jsons.valueToTree(
                MsSqlServerJdbcCtStreamState(
                    ctVersion = ctVersion,
                    pkName = null,
                    pkVal = null,
                )
            )

        /**
         * State saved after a successful CT incremental read. [ctVersion] advances to the current
         * CT version at the time the read started.
         */
        fun incrementalCheckpoint(newCtVersion: Long): OpaqueStateValue =
            Jsons.valueToTree(
                MsSqlServerJdbcCtStreamState(
                    ctVersion = newCtVersion,
                    pkName = null,
                    pkVal = null,
                )
            )
    }
}
