Fluss
----

FLUSS is a **FL**ink **U**nified **S**treaming **S**torage.

## Development

### Code Formatting

Fluss uses [Spotless](https://github.com/diffplug/spotless/tree/main/plugin-maven) together with [google-java-format](https://github.com/google/google-java-format) to format the Java code.
Please see the [documents](https://nightlies.apache.org/flink/flink-docs-master/docs/flinkdev/ide_setup/#code-formatting) to setup the code formatting in your IDE. Please make sure you are using [google-java-format v1.7.0.6](https://plugins.jetbrains.com/plugin/8527-google-java-format/versions/stable/115957) and never update this plugin.

### Testing

Fluss uses [AssertJ](https://assertj.github.io/doc/) as testing assertion framework ([why](https://flink.apache.org/how-to-contribute/code-style-and-quality-common/#tooling)). Please do not use JUnit assertions, Hamcrest matchers, Mockito assertions or any other assertion framework.
If you have a lot of JUnit assertions in code, you can use [Assertions2Assertj](https://plugins.jetbrains.com/plugin/10345-assertions2assertj) IntelliJ IDEA plugin to easy convert Junit assertions into AssertJ.
For conversion, you can right-click on the test file in IntelliJ IDEA and select `Refactor` -> `Convert Assertions to AssertJ` -> `Convert current file`.

Besides, [please avoid using @Timeout in JUnit tests](https://flink.apache.org/how-to-contribute/code-style-and-quality-common/#avoid-timeouts-in-junit-tests).

#### Code Coverage

Fluss uses [JaCoCo](https://www.eclemma.org/jacoco/) to measure code coverage. Please make sure your code is covered by tests and CLASS coverage should not less than 70%.
Otherwise, the compile will fail. You can use the `Run with Coverage` tool of IntelliJ IDEA to improve class coverage.
For special cases, you can exclude class in the `jacoco-check` stage in the root `pom.xml` file.
