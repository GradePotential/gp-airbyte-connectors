/*
 * Copyright (c) 2026 Grade Potential Tutoring. All rights reserved.
 */

package io.airbyte.integrations.source.mssqlct

import io.airbyte.cdk.AirbyteSourceRunner

object MsSqlServerSource {
    @JvmStatic
    fun main(args: Array<String>) {
        AirbyteSourceRunner.run(*args)
    }
}
