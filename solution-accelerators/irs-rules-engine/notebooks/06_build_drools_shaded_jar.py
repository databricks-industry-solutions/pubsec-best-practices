# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Build Drools Shaded JAR
# MAGIC
# MAGIC Builds `drools-dmn-shaded-2.0.0.jar` — a pure Drools/KIE dependency bundle
# MAGIC with no custom code. This JAR puts the Drools 9.x runtime on the cluster
# MAGIC classpath so the inline `%scala DroolsHolder` in `04_batch_scoring` can use it.
# MAGIC
# MAGIC **When to rebuild:**
# MAGIC - Upgrading Drools version (`drools.version` in pom.xml)
# MAGIC - Never needed for rule changes — those go through the DMN file
# MAGIC - Never needed for new input columns — those go through `04_batch_scoring.py`
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Write pom.xml to driver (embedded below — single source of truth)
# MAGIC 2. Download Maven 3.9.9 if not cached
# MAGIC 3. Run `mvn package`
# MAGIC 4. Copy shaded JAR to UC Volume
# MAGIC 5. Restart Drools Runner cluster to pick it up

# COMMAND ----------

import os, shutil, subprocess, sys, glob

CATALOG     = "services_bureau_catalog"
SCHEMA      = "irs_rrp"
VOLUME      = "dmn_rules"
JAR_NAME    = "drools-dmn-shaded-2.0.0.jar"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{JAR_NAME}"
BUILD_DIR   = "/tmp/drools-build"
MVN_HOME    = "/tmp/maven"
MVN_BIN     = f"{MVN_HOME}/bin/mvn"
MVN_VER     = "3.9.9"

# COMMAND ----------

# MAGIC %md ## Step 1 — Write pom.xml to driver

# COMMAND ----------

# pom.xml is embedded here (not synced from a file) because this notebook
# IS the build script — keeping them co-located avoids drift.
# To upgrade Drools, change drools.version here AND in drools-shaded/pom.xml in the repo.

POM_XML = r"""<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>com.irs.rules</groupId>
  <artifactId>drools-dmn-shaded</artifactId>
  <version>2.0.0</version>
  <packaging>jar</packaging>

  <properties>
    <drools.version>9.44.0.Final</drools.version>
    <maven.compiler.source>17</maven.compiler.source>
    <maven.compiler.target>17</maven.compiler.target>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.kie</groupId>
      <artifactId>kie-dmn-core</artifactId>
      <version>${drools.version}</version>
      <exclusions>
        <exclusion><groupId>org.slf4j</groupId><artifactId>*</artifactId></exclusion>
        <exclusion><groupId>ch.qos.logback</groupId><artifactId>*</artifactId></exclusion>
        <exclusion><groupId>com.fasterxml.jackson.core</groupId><artifactId>*</artifactId></exclusion>
        <exclusion><groupId>com.fasterxml.jackson.module</groupId><artifactId>*</artifactId></exclusion>
        <exclusion><groupId>com.fasterxml.jackson.datatype</groupId><artifactId>*</artifactId></exclusion>
      </exclusions>
    </dependency>
    <dependency>
      <groupId>org.kie</groupId>
      <artifactId>kie-dmn-feel</artifactId>
      <version>${drools.version}</version>
      <exclusions>
        <exclusion><groupId>org.slf4j</groupId><artifactId>*</artifactId></exclusion>
      </exclusions>
    </dependency>
    <dependency>
      <groupId>org.drools</groupId>
      <artifactId>drools-engine</artifactId>
      <version>${drools.version}</version>
      <exclusions>
        <exclusion><groupId>org.slf4j</groupId><artifactId>*</artifactId></exclusion>
      </exclusions>
    </dependency>
    <dependency>
      <groupId>org.drools</groupId>
      <artifactId>drools-mvel</artifactId>
      <version>${drools.version}</version>
      <exclusions>
        <exclusion><groupId>org.slf4j</groupId><artifactId>*</artifactId></exclusion>
      </exclusions>
    </dependency>
    <dependency>
      <groupId>org.drools</groupId>
      <artifactId>drools-core</artifactId>
      <version>${drools.version}</version>
      <exclusions>
        <exclusion><groupId>org.slf4j</groupId><artifactId>*</artifactId></exclusion>
        <exclusion><groupId>commons-codec</groupId><artifactId>*</artifactId></exclusion>
      </exclusions>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.1</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <createDependencyReducedPom>false</createDependencyReducedPom>
              <relocations>
                <relocation><pattern>org.antlr</pattern><shadedPattern>drools.shaded.antlr</shadedPattern></relocation>
                <relocation><pattern>org.eclipse</pattern><shadedPattern>drools.shaded.eclipse</shadedPattern></relocation>
                <relocation><pattern>com.thoughtworks.xstream</pattern><shadedPattern>drools.shaded.xstream</shadedPattern></relocation>
                <relocation><pattern>org.apache.commons</pattern><shadedPattern>drools.shaded.commons</shadedPattern></relocation>
              </relocations>
              <filters>
                <filter>
                  <artifact>*:*</artifact>
                  <excludes>
                    <exclude>META-INF/*.SF</exclude>
                    <exclude>META-INF/*.DSA</exclude>
                    <exclude>META-INF/*.RSA</exclude>
                    <exclude>org/slf4j/**</exclude>
                    <exclude>module-info.class</exclude>
                    <exclude>**/module-info.class</exclude>
                    <exclude>com/fasterxml/**</exclude>
                  </excludes>
                </filter>
              </filters>
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                  <resource>META-INF/kie.conf</resource>
                </transformer>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
"""

os.makedirs(BUILD_DIR, exist_ok=True)
with open(f"{BUILD_DIR}/pom.xml", "w") as f:
    f.write(POM_XML)
print(f"✅ pom.xml written to {BUILD_DIR}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Install Maven

# COMMAND ----------

def run(cmd, cwd=None, check=True):
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True, cwd=cwd)
    if result.stdout: print(result.stdout)
    if result.stderr: print(result.stderr, file=sys.stderr)
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed (exit {result.returncode}): {cmd}")
    return result

if not os.path.exists(MVN_BIN):
    print(f"Downloading Maven {MVN_VER}...")
    MVN_URL = f"https://archive.apache.org/dist/maven/maven-3/{MVN_VER}/binaries/apache-maven-{MVN_VER}-bin.tar.gz"
    run(f"curl -fsSL {MVN_URL} -o /tmp/maven.tar.gz")
    run(f"tar -xzf /tmp/maven.tar.gz -C /tmp")
    run(f"mv /tmp/apache-maven-{MVN_VER} {MVN_HOME}")
else:
    print("Maven already cached.")

os.environ["PATH"] = f"{MVN_HOME}/bin:" + os.environ.get("PATH", "")
run(f"{MVN_BIN} --version")

# COMMAND ----------

# MAGIC %md ## Step 3 — Build

# COMMAND ----------

log_file = "/tmp/mvn-build.log"
result = subprocess.run(
    f"{MVN_BIN} package -DskipTests 2>&1 | tee {log_file}",
    shell=True, cwd=BUILD_DIR, text=True,
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT
)
with open(log_file) as f:
    log = f.read()
tail = log[-6000:] if len(log) > 6000 else log
print(tail)

if result.returncode != 0:
    dbutils.notebook.exit(f"FAILED (exit {result.returncode})\n\n{tail}")

run(f"ls -lh {BUILD_DIR}/target/")

# COMMAND ----------

# MAGIC %md ## Step 4 — Copy JAR to UC Volume

# COMMAND ----------

candidates  = glob.glob(f"{BUILD_DIR}/target/*.jar")
shaded_jars = [j for j in candidates if "original" not in j]

if not shaded_jars:
    dbutils.notebook.exit(f"FAILED: No shaded JAR in target/. Found: {candidates}")

local_jar = shaded_jars[0]
shutil.copy2(local_jar, VOLUME_PATH)
size_mb = os.path.getsize(VOLUME_PATH) / 1024 / 1024
print(f"✅ {JAR_NAME} → {VOLUME_PATH}  ({size_mb:.1f} MB)")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: {JAR_NAME} built and uploaded to {VOLUME_PATH}")
