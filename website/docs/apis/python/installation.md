---
sidebar_position: 1
---
# Installation

```bash
pip install pyfluss
```

## Building From Source (Optional)

**Prerequisites:** Python 3.9+, Rust 1.85+

```bash
git clone https://github.com/apache/fluss-rust.git
cd fluss-rust/bindings/python
```

Install [maturin](https://github.com/PyO3/maturin):

```bash
pip install maturin
```

Build and install:

```bash
# Development mode (editable)
maturin develop

# Or build a wheel
maturin build --release
pip install target/wheels/fluss-*.whl
```

Verify:

```python
import fluss
print("Fluss Python bindings installed successfully!")
```
