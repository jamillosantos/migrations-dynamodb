package migrations_dynamodb

type opts struct {
	lockID        string
	lockTableName string
	tableName     string
}

func defaultOpts() opts {
	return opts{
		lockID:        "migrations",
		tableName:     "_migrations",
		lockTableName: "_migrations-lock",
	}
}

type Option func(*opts)

// WithLockID sets the lock ID to be used by the DynamoDB lock.
func WithLockID(lockID string) Option {
	return func(o *opts) {
		o.lockID = lockID
	}
}

// WithLockTableName sets the lock table name to be used by the DynamoDB lock.
func WithLockTableName(lockTableName string) Option {
	return func(o *opts) {
		o.lockTableName = lockTableName
	}
}

// WithTableName sets the table name to record the database migrations ran.
func WithTableName(tableName string) Option {
	return func(o *opts) {
		o.tableName = tableName
	}
}
