# BDA Final Project - Environment Summary

This file documents the environment used to run the project pipeline.

## Key Components
- **Operating System:** `Linux-6.6.87.2-microsoft-standard-WSL2-x86_64-with-glibc2.39`
- **Python Version:** `3.10.19`
- **PySpark Version:** `4.0.1`
- **Apache Spark Version:** `4.0.1`
- **Java Version:** `openjdk version "21.0.6-internal" 2025-01-21`

## Spark Configuration

- `spark.app.id`: `local-1763207948257`
- `spark.app.name`: `BDA Final Project - Bitcoin Predictor`
- `spark.app.startTime`: `1763207947363`
- `spark.app.submitTime`: `1763207946921`
- `spark.driver.extraJavaOptions`: `-Djava.net.preferIPv6Addresses=false -XX:+IgnoreUnrecognizedVMOptions --add-modules=jdk.incubator.vector --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true`
- `spark.driver.host`: `10.255.255.254`
- `spark.driver.memory`: `4g`
- `spark.driver.port`: `43333`
- `spark.executor.extraJavaOptions`: `-Djava.net.preferIPv6Addresses=false -XX:+IgnoreUnrecognizedVMOptions --add-modules=jdk.incubator.vector --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true`
- `spark.executor.id`: `driver`
- `spark.hadoop.fs.s3a.vectored.read.max.merged.size`: `2M`
- `spark.hadoop.fs.s3a.vectored.read.min.seek.size`: `128K`
- `spark.master`: `local[*]`
- `spark.rdd.compress`: `True`
- `spark.serializer.objectStreamReset`: `100`
- `spark.sql.artifact.isolation.enabled`: `false`
- `spark.submit.deployMode`: `client`
- `spark.submit.pyFiles`: ``
- `spark.ui.showConsoleProgress`: `true`
