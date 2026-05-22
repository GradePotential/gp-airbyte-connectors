/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mssqlct

import io.airbyte.cdk.ConfigErrorException
import io.airbyte.cdk.command.*
import io.airbyte.cdk.jdbc.SSLCertificateUtils
import io.airbyte.cdk.output.DataChannelMedium
import io.airbyte.cdk.output.sockets.DATA_CHANNEL_PROPERTY_PREFIX
import io.airbyte.cdk.ssh.SshConnectionOptions
import io.airbyte.cdk.ssh.SshNoTunnelMethod
import io.airbyte.cdk.ssh.SshTunnelMethodConfiguration
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.time.Duration
import org.apache.commons.lang3.RandomStringUtils

private val log = KotlinLogging.logger {}

class MsSqlServerSourceConfiguration(
    override val realHost: String,
    override val realPort: Int,
    override val sshTunnel: SshTunnelMethodConfiguration?,
    override val sshConnectionOptions: SshConnectionOptions,
    override val jdbcUrlFmt: String,
    override val jdbcProperties: Map<String, String>,
    override val namespaces: Set<String>,
    override val maxConcurrency: Int,
    override val resourceAcquisitionHeartbeat: Duration = Duration.ofMillis(100L),
    override val checkpointTargetInterval: Duration,
    override val checkPrivileges: Boolean,
    val incrementalReplicationConfiguration: IncrementalConfiguration,
    val databaseName: String,
) : JdbcSourceConfiguration {
    // Change Tracking mode is per-stream (not global); cursor mode is also per-stream.
    override val global = false
    override val maxSnapshotReadDuration: Duration? = null

    /** Required to inject [MsSqlServerSourceConfiguration] directly. */
    @Factory
    private class MicronautFactory {
        @Singleton
        fun mssqlServerSourceConfig(
            factory:
                SourceConfigurationFactory<
                    MsSqlServerSourceConfigurationSpecification, MsSqlServerSourceConfiguration>,
            supplier:
                ConfigurationSpecificationSupplier<MsSqlServerSourceConfigurationSpecification>,
        ): MsSqlServerSourceConfiguration = factory.make(supplier.get())
    }
}

sealed interface IncrementalConfiguration

data class UserDefinedCursorIncrementalConfiguration(val excludeTodaysData: Boolean = false) :
    IncrementalConfiguration

/**
 * Change Tracking incremental configuration. Uses SQL Server's CHANGE_TRACKING_CURRENT_VERSION()
 * and CHANGETABLE(CHANGES ...) to detect inserts, updates, and deletes without requiring CDC or
 * SQL Server Agent.
 */
data class ChangeTrackingIncrementalConfiguration(
    val invalidCtVersionBehavior: InvalidCtVersionBehavior = InvalidCtVersionBehavior.RESET_SYNC,
    val initialLoadTimeout: Duration = Duration.ofHours(8),
) : IncrementalConfiguration

enum class InvalidCtVersionBehavior {
    /** Fail the sync if the stored CT version is no longer valid (outside retention window). */
    FAIL_SYNC,
    /** Reset and re-sync data from scratch if the stored CT version has expired. */
    RESET_SYNC,
}

@Singleton
class MsSqlServerSourceConfigurationFactory
@Inject
constructor(
    val featureFlags: Set<FeatureFlag>,
    @Value("\${${DATA_CHANNEL_PROPERTY_PREFIX}.medium}")
    val dataChannelMedium: String = DataChannelMedium.STDIO.name,
    @Value("\${${DATA_CHANNEL_PROPERTY_PREFIX}.socket-paths}")
    val socketPaths: List<String> = emptyList(),
) :
    SourceConfigurationFactory<
        MsSqlServerSourceConfigurationSpecification, MsSqlServerSourceConfiguration> {

    constructor() : this(emptySet(), DataChannelMedium.STDIO.name, emptyList())

    override fun makeWithoutExceptionHandling(
        pojo: MsSqlServerSourceConfigurationSpecification,
    ): MsSqlServerSourceConfiguration {
        val incrementalSpec = pojo.getIncrementalValue()
        val incrementalReplicationConfiguration =
            when (incrementalSpec) {
                is UserDefinedCursor -> {
                    UserDefinedCursorIncrementalConfiguration(
                        excludeTodaysData = incrementalSpec.excludeTodaysData ?: false
                    )
                }
                is ChangeTracking -> {
                    val invalidCtVersionBehavior =
                        if (incrementalSpec.invalidCtVersionBehavior == "Fail sync") {
                            InvalidCtVersionBehavior.FAIL_SYNC
                        } else {
                            InvalidCtVersionBehavior.RESET_SYNC
                        }
                    val initialLoadTimeout =
                        Duration.ofHours(incrementalSpec.initialLoadTimeoutHours?.toLong() ?: 8L)
                    ChangeTrackingIncrementalConfiguration(
                        invalidCtVersionBehavior = invalidCtVersionBehavior,
                        initialLoadTimeout = initialLoadTimeout,
                    )
                }
            }

        val sshTunnel: SshTunnelMethodConfiguration? = pojo.getTunnelMethodValue()

        val isLegacyConfig = pojo.encryptionJson == null
        val jdbcEncryption =
            when (val encryptionSpec: EncryptionSpecification? = pojo.getEncryptionValue()) {
                is MsSqlServerEncryptionDisabledConfigurationSpecification -> {
                    if (isLegacyConfig) {
                        log.warn {
                            "No encryption configuration found in JSON. " +
                                "This appears to be a legacy configuration. " +
                                "Consider adding SSL encryption for better security."
                        }
                        mapOf("encrypt" to "false", "trustServerCertificate" to "true")
                    } else {
                        if (
                            featureFlags.contains(FeatureFlag.AIRBYTE_CLOUD_DEPLOYMENT) &&
                                sshTunnel is SshNoTunnelMethod
                        ) {
                            throw ConfigErrorException(
                                "Connection from Airbyte Cloud requires " +
                                    "SSL encryption or an SSH tunnel."
                            )
                        } else {
                            mapOf("encrypt" to "false", "trustServerCertificate" to "true")
                        }
                    }
                }
                null -> {
                    mapOf("encrypt" to "false", "trustServerCertificate" to "true")
                }
                is MsSqlServerEncryptionRequiredTrustServerCertificateConfigurationSpecification ->
                    mapOf("encrypt" to "true", "trustServerCertificate" to "true")
                is SslVerifyCertificate -> {
                    val certificate = encryptionSpec.certificate
                    val trustStoreProperties =
                        if (certificate == null) {
                            emptyMap()
                        } else {
                            val password = RandomStringUtils.secure().next(100)
                            val keyStoreUri =
                                SSLCertificateUtils.keyStoreFromCertificate(certificate, password)
                            mapOf(
                                "trustStore" to keyStoreUri.path,
                                "trustStorePassword" to password
                            )
                        }
                    val hostNameInCertificate = encryptionSpec.hostNameInCertificate
                    val hostNameProperties =
                        if (hostNameInCertificate == null) {
                            emptyMap()
                        } else {
                            mapOf("hostNameInCertificate" to hostNameInCertificate)
                        }
                    trustStoreProperties +
                        hostNameProperties +
                        mapOf("encrypt" to "true", "trustServerCertificate" to "false")
                }
            }

        val jdbcProperties = mutableMapOf<String, String>()
        jdbcProperties["user"] = pojo.username
        jdbcProperties["password"] = pojo.password

        val pattern = "^([^=]+)=(.*)$".toRegex()
        for (pair in (pojo.jdbcUrlParams ?: "").trim().split("&".toRegex())) {
            if (pair.isBlank()) {
                continue
            }
            val result: MatchResult? = pattern.matchEntire(pair)
            if (result == null) {
                log.warn { "ignoring invalid JDBC URL param '$pair'" }
            } else {
                val key: String = result.groupValues[1].trim()
                val urlEncodedValue: String = result.groupValues[2].trim()
                jdbcProperties[key] = URLDecoder.decode(urlEncodedValue, StandardCharsets.UTF_8)
            }
        }
        jdbcProperties.putAll(jdbcEncryption)

        val checkpointTargetInterval: Duration =
            Duration.ofSeconds(pojo.checkpointTargetIntervalSeconds?.toLong() ?: 300L)
        if (!checkpointTargetInterval.isPositive) {
            throw ConfigErrorException("Checkpoint Target Interval should be positive")
        }

        var maxConcurrency: Int? = pojo.concurrency

        log.info { "maxConcurrency: $maxConcurrency. socket paths: ${socketPaths.size}" }

        maxConcurrency =
            when (DataChannelMedium.valueOf(dataChannelMedium)) {
                DataChannelMedium.STDIO -> maxConcurrency ?: 1
                DataChannelMedium.SOCKET -> maxConcurrency ?: socketPaths.size
            }
        log.info { "Effective concurrency: $maxConcurrency" }

        if (maxConcurrency <= 0) {
            throw ConfigErrorException("Concurrency setting should be positive")
        }

        return MsSqlServerSourceConfiguration(
            realHost = pojo.host,
            realPort = pojo.port,
            sshTunnel = sshTunnel,
            sshConnectionOptions = SshConnectionOptions.fromAdditionalProperties(emptyMap()),
            checkpointTargetInterval = checkpointTargetInterval,
            jdbcUrlFmt = "jdbc:sqlserver://%s:%d;databaseName=${pojo.database}",
            namespaces = pojo.schemas?.takeIf { it.isNotEmpty() }?.toSet() ?: emptySet(),
            jdbcProperties = jdbcProperties,
            maxConcurrency = maxConcurrency,
            checkPrivileges = pojo.checkPrivileges ?: true,
            incrementalReplicationConfiguration = incrementalReplicationConfiguration,
            databaseName = pojo.database
        )
    }
}
