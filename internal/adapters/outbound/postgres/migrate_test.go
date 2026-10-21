package postgres

import "testing"

// TestMigrationsURL pins the PgBouncer rule: DDL runs against
// MIGRATIONS_DATABASE_URL when it is set and non-empty, and falls back to
// DATABASE_URL (which may point at a pooler) otherwise.
func TestMigrationsURL(t *testing.T) {
	cases := []struct {
		name   string
		getenv func(string) string
		want   string
	}{
		{
			name: "migrations url wins",
			getenv: func(k string) string {
				return map[string]string{
					EnvMigrationsDatabaseURL: "postgres://direct:5432/exceptions",
					EnvDatabaseURL:           "postgres://pgbouncer:6432/exceptions",
				}[k]
			},
			want: "postgres://direct:5432/exceptions",
		},
		{
			name: "empty migrations url falls back to database url",
			getenv: func(k string) string {
				return map[string]string{
					EnvMigrationsDatabaseURL: "",
					EnvDatabaseURL:           "postgres://pgbouncer:6432/exceptions",
				}[k]
			},
			want: "postgres://pgbouncer:6432/exceptions",
		},
		{
			name:   "unset migrations url falls back to database url",
			getenv: func(k string) string { return "postgres://only:5432/exceptions" },
			want:   "postgres://only:5432/exceptions",
		},
		{
			name:   "nothing set is empty",
			getenv: func(string) string { return "" },
			want:   "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := MigrationsURL(tc.getenv); got != tc.want {
				t.Fatalf("MigrationsURL = %q, want %q", got, tc.want)
			}
		})
	}
}
