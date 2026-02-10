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

@Library(['meetPaasJenkinsLib','ciHelper@master']) _

/**
 * Build version tag helper
 */
def imageTag() {
    def gitHash = sh(returnStdout: true, script: "git log -n 1 --pretty=format:'%h'").trim()
    return "0.8-${gitHash}"
}

/**
 * Constants
 */
class Constants {
    static final String DIRECTORY_BACKEND = ''

    // TODO: update repo paths for fluss
    static final String FLUSS_ECR_REPO = '/wap-dataprocessor/fluss'
    static final String AWS_CONTAINER_REGISTRY = '527856644868.dkr.ecr.us-east-2.amazonaws.com/webex-wap'

    static final String VAULT_CRED_ID = 'c3b8bd92-f54c-4ed5-be88-f438dfa066e6'
    static final String VAULT_NAMESPACE = 'meetpaas'
    static final String AWS_ROLE = 'wap-ecr-readwrite'

    // Optional maven settings, if needed
    static final String MAVEN_SETTING = 'bdb92e53-4de9-4335-aa45-065b69a0d624'
}

/**
 * Anchore scan
 */
def scan(imageTag) {
    sh """
        ${env.WORKSPACE}/bin/syft -o json ${imageTag} | \
        ${env.WORKSPACE}/bin/anchorectl image add ${imageTag} --wait --from -
    """

    def status = sh(
        returnStdout: true,
        script: """${env.WORKSPACE}/bin/anchorectl image check ${imageTag} \
                   | grep '^Evaluation' | awk '{print \$2}'"""
    ).trim()

    if (status == 'fail') {
        error("Anchore scan failed for image: ${imageTag}")
    }
}

pipeline {
    agent { label 'wap-agents' }

    // JDK is set via withMaven in Build Package

    environment {
        ENABLE_ANCHORE = "false"
        ENABLE_IMAGE_PIPELINE = "false"
        ANCHORECTL_ACCOUNT = "webex-wap"
        ANCHORECTL_URL     = "https://anchore.int.acmhwxt-prd-1.prod.infra.webex.com"
        ANCHORECTL_USERNAME = credentials("UDP_ANCHORE_USERNAME")
        ANCHORECTL_PASSWORD = credentials("UDP_ANCHORE_PASSWORD")
    }

    stages {
        /**
         * Install Anchore tools
         */
        stage('Setup Anchore') {
            when {
                expression { env.ENABLE_ANCHORE == 'true' }
            }
            steps {
                sh "echo 'Setup Anchore tools...'"
                sh "mkdir -p ${env.WORKSPACE}/bin"

                retry(3) {
                    sh "curl -sSfL https://anchorectl-releases.anchore.io/anchorectl/install.sh \
                         | sh -s -- -b ${env.WORKSPACE}/bin v5.3.0"
                }

                retry(3) {
                    sh """
                        curl -sSfL https://raw.githubusercontent.com/anchore/syft/main/install.sh \
                        | sh -s -- -b ${env.WORKSPACE}/bin \
                          \$(${env.WORKSPACE}/bin/anchorectl version | grep SyftVersion | awk '{print \$2}')
                    """
                }
            }
        }

        /**
         * Checkout current repo
         */
        stage('Checkout') {
            steps { checkout scm }
        }

        /**
         * Build Package (Java 17, tests skipped)
         */
        stage('Build Package') {
            steps {
                withMaven(
                    jdk: 'OpenJDK-17',
                    maven: 'Maven 3.6.1',
                    options: [
                        artifactsPublisher(disabled: true),
                        junitPublisher(disabled: true)
                    ]) {

                    sh '''
                        set -euxo pipefail

                        # Jenkins 的专用临时目录：不在 repo 树里（避免 RAT），一般也不会 noexec
                        CI_TMP="${WORKSPACE}@tmp/fluss-tmp-${BUILD_NUMBER}"
                        NETTY_NATIVE="${CI_TMP}/netty-native"
                        trap 'rm -rf "${CI_TMP}" || true' EXIT

                        rm -rf "${CI_TMP}" || true
                        mkdir -p "${NETTY_NATIVE}"
                        chmod 755 "${WORKSPACE}@tmp" "${CI_TMP}" "${NETTY_NATIVE}" || true

                        # 让所有 Java 进程继承
                        export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} \
                          -Djava.io.tmpdir=${CI_TMP} \
                          -Dorg.apache.fluss.shaded.netty4.io.netty.native.workdir=${NETTY_NATIVE} \
                          -Dcom.github.luben.zstd.tmpdir=${CI_TMP} \
                          --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
                          --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED"

                        MAVEN_REACTOR_EXCLUDES="\
!fluss-metrics/fluss-metrics-influxdb,\
!fluss-flink/fluss-flink-1.19,\
!fluss-flink/fluss-flink-1.20,\
!fluss-flink/fluss-flink-tiering,\
!fluss-spark/fluss-spark-common,\
!fluss-spark/fluss-spark-ut,\
!fluss-spark/fluss-spark-3.5,\
!fluss-spark/fluss-spark-3.4,\
!fluss-spark,\
!fluss-lake/fluss-lake-paimon,\
!fluss-lake/fluss-lake-iceberg,\
!fluss-lake/fluss-lake-lance,\
!fluss-lake/fluss-lake-hudi,\
!fluss-lake,\
!fluss-kafka,\
!fluss-jmh,\
!fluss-filesystems/fluss-fs-oss,\
!fluss-filesystems/fluss-fs-gs,\
!fluss-filesystems/fluss-fs-azure,\
!fluss-filesystems/fluss-fs-obs,\
!fluss-filesystems/fluss-fs-cos,\
!fluss-filesystems/fluss-fs-hdfs,\
!fluss-test-coverage"

                        ./mvnw -B -V -T 1C \
                          -DskipTests \
                          -DskipOptionalDistPlugins \
                          -Ddist.plugins.descriptor=src/main/assemblies/plugins-slim.xml \
                          -Ddist.package.classifier=-slim \
                          -pl "${MAVEN_REACTOR_EXCLUDES}" \
                          clean package
                    '''
                }
            }
        }

        /**
         * Archive binary package
         */
        stage('Archive Package') {
            steps {
                sh "ls -lh fluss-dist/target/*-slim-bin.tgz"
                archiveArtifacts artifacts: 'fluss-dist/target/*-slim-bin.tgz', fingerprint: true, onlyIfSuccessful: true
            }
        }


        /**
         * Build Image
         */
        stage('Build Image') {
            when {
                expression { env.ENABLE_IMAGE_PIPELINE == 'true' }
            }
            steps {
                script {
                    def tag = imageTag()
                    def ecrPath = "${Constants.AWS_CONTAINER_REGISTRY}${Constants.FLUSS_ECR_REPO}:${tag}"

                    dir("docker/fluss") {
                        sh """
                            DOCKER_BUILDKIT=0 docker image build . \
                              -f Dockerfile \
                              -t ${ecrPath}
                        """
                    }
                }
            }
        }

        /**
         * Scan Image
         */
        stage('Scan Image') {
            when {
                expression { env.ENABLE_ANCHORE == 'true' && env.ENABLE_IMAGE_PIPELINE == 'true' }
            }
            steps {
                script {
                    def tag = imageTag()
                    def ecrPath = "${Constants.AWS_CONTAINER_REGISTRY}${Constants.FLUSS_ECR_REPO}:${tag}"
                    scan(ecrPath)
                }
            }
        }

        /**
         * Push Image
         */
        stage('Push Image') {
            when {
                expression { env.ENABLE_IMAGE_PIPELINE == 'true' }
            }
            steps {
                script {
                    def tag = imageTag()
                    def ecrPath = "${Constants.AWS_CONTAINER_REGISTRY}${Constants.FLUSS_ECR_REPO}:${tag}"

                    dir(Constants.DIRECTORY_BACKEND) {
                        ecr.withRegistry(Constants.VAULT_CRED_ID, Constants.VAULT_NAMESPACE, Constants.AWS_ROLE) {
                            // Login Artifactory/ECR if needed. Replace credentialsId accordingly.
                            withCredentials([usernamePassword(credentialsId: 'cloud9-password-xiaohzho',
                                             usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
                                sh """
                                    docker login --username=${USERNAME} --password=${PASSWORD} artifactory.devhub-cloud.cisco.com
                                    docker push ${ecrPath}
                                """
                            }
                        }
                    }
                }
            }
        }

        /**
         * Cleanup
         */
        stage('Cleanup') {
            steps {
                sh "echo 'Cleanup workspace...'"
                deleteDir()
            }
        }
    }
}
