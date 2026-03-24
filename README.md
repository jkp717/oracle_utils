# Oracle SQL Utils

A robust Python utility library designed for seamless **Oracle Database** interaction and automated **SQL transformation**. It bridges the gap between Oracle-specific syntax and modern dialects like PostgreSQL using the `sqlglot` framework.

## Features

* **Hybrid Connection Modes**: Supports Oracle **Thin mode** (no client binary required) and **Thick mode** (via Oracle Instant Client).
* **Secure Credential Management**: Integrated `python-dotenv` support for managing DB secrets via `.env` files.
* **Smart SQL Translation**:
    * Converts `LISTAGG` to `STRING_AGG`.
    * Translates Oracle `ROWNUM` filtering to standard `LIMIT` clauses.
    * Handles Oracle `PARTITION` comments and converts them to actionable `WHERE` conditions.
* **Automated Star Expansion**: Queries live database metadata to expand `SELECT *` into explicit column lists.
* **Alias Normalization**: Enforces consistent casing (uppercase) for top-level SQL aliases.

---

## Installation

1.  **Clone the repository** and navigate to the root directory.
2.  **Install the package** in editable mode:
    ```bash
    pip install -e .
    ```

### Dependencies
* `oracledb`
* `sqlglot`
* `python-dotenv`

---

## Configuration

Create a `.env` file in your project root to store your database configuration:

```env
ORACLE_USERNAME=your_username
ORACLE_PASSWORD=your_password
ORACLE_HOST=your_host_address
ORACLE_PORT=1521
ORACLE_SERVICE=your_service_name

# Optional: Required only for Thick Mode
ORACLE_CLIENT_LIB=C:\path\to\instantclient_19_8