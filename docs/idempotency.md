# Idempotency in owopay

## 1. What is idempotency?

Idempotency means that repeating an operation produces the same intended final result as performing it once.

Without idempotency, retries could insert duplicate transactions and produce inaccurate reports.

## 2. Why is it important?

Data pipelines can run more than once because of:
- Airflow task retries.
- Network interruptions.
- Database connection failures.
- Manual reruns after an unsuccessful execution.

Idempotency ensures that repeated extraction does not multiply transaction records.

## 3. Implementation

The extraction process uses the following protections:

### Delete and reload

Before loading a business day, existing records for that day are deleted. The latest records are then copied from the source database.

### Database transaction

The extraction runs inside a database transaction. If an error occurs, the transaction is rolled back, preserving the previous warehouse data.

### Unique transaction ID

A unique index on `raw.transactions(txn_id)` prevents duplicate transaction IDs from being stored in the warehouse.

### PostgreSQL advisory lock

A transaction-scoped advisory lock ensures that only one extraction process using this lock can write at a time. This prevents overlapping extraction runs from interfering with each other.

## 4. Expected behaviour

For example, suppose an extraction loads 500 transactions for a business day.

If the same extraction runs again, it replaces the existing day's records instead of adding another 500 records.

The expected result remains 500 records, assuming the source data has not changed.

## 5. Testing

The implementation should be checked by:
- Running the extraction for a business day.
- Running it again for the same day.
- Comparing the transaction count.
- Confirming that duplicate transaction IDs are rejected.
- Confirming that a failed extraction rolls back its changes.

## 7. Conclusion

Idempotency makes the OwoPay data pipeline safer to retry and rerun. The combination of day-level replacement, database transactions, a unique transaction ID index and an advisory lock helps protect warehouse data integrity.