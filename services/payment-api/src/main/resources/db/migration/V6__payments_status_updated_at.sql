-- UX round 2: the lifecycle view needs to know WHEN the outcome landed, not just what it is.
-- Set by the status listener's UPDATE (see PaymentJpaRepository#updateStatus); NULL for rows
-- whose outcome arrived before this column existed, and for payments still in CREATED.
ALTER TABLE payments ADD COLUMN status_updated_at timestamptz;
