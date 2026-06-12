# Apache Fluss™ Rust (Incubating)

![Experimental](https://img.shields.io/badge/status-experimental-orange)

Rust implementation of [Apache Fluss™](https://fluss.apache.org/).


## Why Fluss?
[Fluss](https://fluss.apache.org/) is a streaming storage built for real-time analytics which can serve as the real-time data layer for Lakehouse architectures.
It bridges the gap between streaming data and the data Lakehouse by enabling low-latency, high-throughput data ingestion and processing while seamlessly integrating with popular compute engines.

## Why Fluss Rust Client
It's an official Rust client for interacting with Fluss. This client provides foundational capabilities for table management and log streaming operations, enabling developers to explore Fluss within Rust ecosystems.

## Quick-Start

### Step1 Start Fluss cluster
#### Requirements
Fluss runs on all UNIX-like environments, e.g. Linux, Mac OS X. Before you start to setup the system, make sure you have the following software installed on your test machine:

Java 17 or higher (Java 8 and Java 11 are not recommended)
If your cluster does not fulfill these software requirements you will need to install/upgrade it.

Fluss requires the JAVA_HOME environment variable to be set on all nodes and point to the directory of your Java installation.

#### Fluss Setup
Go to the [downloads](https://fluss.apache.org/downloads/) page and download the latest Fluss release (currently 0.8.0). Make sure to pick the Fluss package matching your Java version. After downloading the latest release, extract it:
```shell
tar -xzf fluss-0.8.0-incubating-bin.tgz
cd fluss-0.8.0-incubating/
```
You can start Fluss local cluster by running the following command:
```shell
./bin/local-cluster.sh start
```
After that, the Fluss local cluster is started.

### Run Provided Example
Only supports Linux or macOs. You will need to [install Rust](https://www.rust-lang.org/tools/install) firstly. 

After that, go the project directory, build it and run the example:
```shell
cargo build --example example-table --release
cd target/release/examples
./example-table
```
The example code is as follows:
```rust
#[tokio::main]
pub async fn main() -> Result<()> {
    // 1: create the table;
    let mut args = Args::default();
    args.bootstrap_servers = "127.0.0.1:9123".to_string();
    let conn_config = ConnectionConfig::from_args(args);
    let conn = FlussConnection::new(conn_config).await;

    let admin = conn.get_admin();

    let table_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("c1", DataTypes::int())
                .column("c2", DataTypes::string())
                .build(),
        )
        .build();

    let table_path = TablePath::new("fluss".to_owned(), "rust_test".to_owned());

    admin
        .create_table(&table_path, &table_descriptor, true)
        .await
        .unwrap();

    // 2: get the table
    let table_info = admin.get_table_info(&table_path).await.unwrap();
    print!("Get created table:\n {}\n", table_info);

    // let's sleep 2 seconds to wait leader ready
    thread::sleep(Duration::from_secs(2));

    // 3: append log to the table
    let table = conn.get_table(&table_path).await;
    let append_writer = table.new_append().create_writer();
    let batch = record_batch!(("c1", Int32, [1, 2, 3, 4, 5, 6]), ("c2", Utf8, ["a1", "a2", "a3", "a4", "a5", "a6"])).unwrap();
    append_writer.append(batch)?;
    append_writer.flush().await?;
    println!("Start to scan log records......");
    // 4: scan the records
    let log_scanner = table.new_scan().create_log_scanner();
    log_scanner.subscribe(0, 0).await;

    loop {
        let scan_records = log_scanner.poll(Duration::from_secs(10)).await?;
        println!("Start to poll records......");
        for record in scan_records {
            let row = record.row();
            println!(
                "{{{}, {}}}@{}",
                row.get_int(0),
                row.get_string(1),
                record.offset()
            );
        }
    }
    Ok(())
}
```

You can change it according to your needs, have fun!

#### Clear environment
Then, stop your Fluss cluster. Go to your Fluss home, stop it via the following commands:
```shell
./bin/local-cluster.sh stop
```

## Documentation

- [Development Guide](DEVELOPMENT.md) – Build, test, and contribute to fluss-rust.
- [Release Guide](website/docs/release/create-release.md) – How to build, release, and sign official Fluss client packages (Rust, Python, C++).

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)