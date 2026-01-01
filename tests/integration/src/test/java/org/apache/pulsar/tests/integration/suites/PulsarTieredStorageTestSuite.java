/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.tests.integration.suites;

import static java.util.stream.Collectors.joining;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testcontainers.containers.BindMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

@Slf4j
public abstract class PulsarTieredStorageTestSuite extends PulsarClusterTestBase {

    private static final String IT_OFFLOADER_IMPL_PROPERTY = "pulsar.it.offloader";
    private static final String IT_OFFLOADER_IMPL_OPENDAL = "opendal";
    private static final String PULSAR_OFFLOADERS_DIR = "/pulsar/offloaders";

    private Path overriddenOffloadersDir;

    protected int getNumEntriesPerLedger() {
        return 1024;
    }

    @BeforeClass(alwaysRun = true)
    @Override
    public final void setupCluster() throws Exception {
        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));

        PulsarClusterSpec spec = PulsarClusterSpec.builder()
            .numBookies(2)
            .numBrokers(1)
            .clusterName(clusterName)
            .build();

        setupCluster(spec);
    }

    @AfterClass(alwaysRun = true)
    @Override
    public final void tearDownCluster() throws Exception {
        super.tearDownCluster();
        cleanupOverriddenOffloadersDir();
    }

    protected abstract Map<String, String> getEnv();

    @Override
    protected void beforeStartCluster() throws Exception {
        super.beforeStartCluster();
        maybeOverrideOffloadersWithOpenDAL();
        for (BrokerContainer brokerContainer : pulsarCluster.getBrokers()) {
            getEnv().forEach(brokerContainer::withEnv);
        }
    }

    private void maybeOverrideOffloadersWithOpenDAL() throws IOException {
        String offloaderImpl = System.getProperty(IT_OFFLOADER_IMPL_PROPERTY, "").trim();
        if (!IT_OFFLOADER_IMPL_OPENDAL.equalsIgnoreCase(offloaderImpl)) {
            return;
        }

        Path openDalNar = findOpenDalOffloaderNar();
        overriddenOffloadersDir = Files.createTempDirectory("pulsar-offloaders-opendal-");
        Files.copy(openDalNar, overriddenOffloadersDir.resolve(openDalNar.getFileName()),
                StandardCopyOption.REPLACE_EXISTING);

        log.info("Overriding broker offloaders with OpenDAL NAR: {} -> {}", openDalNar, overriddenOffloadersDir);
        for (BrokerContainer brokerContainer : pulsarCluster.getBrokers()) {
            brokerContainer.withFileSystemBind(overriddenOffloadersDir.toString(), PULSAR_OFFLOADERS_DIR,
                    BindMode.READ_ONLY);
        }
    }

    private static Path findOpenDalOffloaderNar() throws IOException {
        Path root = findRepoRoot();
        Path targetDir = root.resolve("tiered-storage").resolve("opendal").resolve("target");
        if (!Files.isDirectory(targetDir)) {
            throw new IOException("OpenDAL offloader target dir not found: " + targetDir
                    + " (build it first: mvn -pl tiered-storage/opendal -DskipTests package)");
        }

        try (Stream<Path> stream = Files.list(targetDir)) {
            List<Path> nars = stream
                    .filter(p -> p.getFileName().toString().startsWith("tiered-storage-opendal-"))
                    .filter(p -> p.getFileName().toString().endsWith(".nar"))
                    .sorted(Comparator.comparingLong(PulsarTieredStorageTestSuite::safeLastModifiedMillis))
                    .collect(Collectors.toList());
            if (nars.isEmpty()) {
                throw new IOException("No OpenDAL offloader NAR found under " + targetDir
                        + " (build it first: mvn -pl tiered-storage/opendal -DskipTests package)");
            }
            return nars.get(nars.size() - 1);
        }
    }

    private static Path findRepoRoot() throws IOException {
        // Allow explicit override for custom layouts.
        String explicit = System.getProperty("pulsar.repo.root");
        if (explicit != null && !explicit.isBlank()) {
            Path root = Paths.get(explicit).toAbsolutePath().normalize();
            if (!Files.isDirectory(root)) {
                throw new IOException("pulsar.repo.root is not a directory: " + root);
            }
            return root;
        }

        // Prefer a Maven-provided path we explicitly export in `tests/integration/pom.xml`.
        // Example value: `${project.build.directory}` = `<repo>/tests/integration/target`.
        String buildDir = System.getProperty("maven.buildDirectory");
        Path start = (buildDir != null && !buildDir.isBlank())
                ? Paths.get(buildDir)
                : Paths.get(System.getProperty("user.dir", "."));

        Path current = start.toAbsolutePath().normalize();
        for (int i = 0; i < 10 && current != null; i++) {
            if (Files.isDirectory(current.resolve("tiered-storage").resolve("opendal"))) {
                return current;
            }
            current = current.getParent();
        }

        throw new IOException("Failed to locate Pulsar repo root from " + start
                + " (set -Dpulsar.repo.root=/path/to/pulsar if needed)");
    }

    private static long safeLastModifiedMillis(Path path) {
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (IOException e) {
            return 0;
        }
    }

    private void cleanupOverriddenOffloadersDir() {
        if (overriddenOffloadersDir == null) {
            return;
        }
        if (PulsarContainer.PULSAR_CONTAINERS_LEAVE_RUNNING) {
            log.warn("Not deleting overridden offloaders dir {} because PULSAR_CONTAINERS_LEAVE_RUNNING=true",
                    overriddenOffloadersDir);
            return;
        }
        try (Stream<Path> walk = Files.walk(overriddenOffloadersDir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    log.warn("Failed to delete {}", p, e);
                }
            });
        } catch (IOException e) {
            log.warn("Failed to cleanup overridden offloaders dir {}", overriddenOffloadersDir, e);
        } finally {
            overriddenOffloadersDir = null;
        }
    }
}
