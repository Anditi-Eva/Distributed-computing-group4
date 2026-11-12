CREATE TABLE IF NOT EXISTS slices (
    slice_id VARCHAR(50) PRIMARY KEY,
    slice_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Durable participant state; PREPARED/PRECOMMITTED survive worker restarts.
CREATE TABLE IF NOT EXISTS distributed_transactions (
    tx_id VARCHAR(64) PRIMARY KEY,
    slice_id VARCHAR(50) NOT NULL,
    slice_type VARCHAR(20) NOT NULL,
    protocol VARCHAR(8) NOT NULL CHECK (protocol IN ('2pc', '3pc')),
    state VARCHAR(20) NOT NULL CHECK (state IN
        ('PREPARED', 'PRECOMMITTED', 'COMMITTED', 'COMPLETED', 'ABORTED')),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- A reservation is a durable uniqueness/serialization point while prepared.
CREATE TABLE IF NOT EXISTS slice_reservations (
    slice_id VARCHAR(50) PRIMARY KEY,
    tx_id VARCHAR(64) NOT NULL UNIQUE REFERENCES distributed_transactions(tx_id) ON DELETE CASCADE
);
