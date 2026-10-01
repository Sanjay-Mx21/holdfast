package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

type testConfig struct {
	Service Service
	HTTP    HTTP
}

func (c *testConfig) Validate() error { return ValidateAll(c.Service, c.HTTP) }

func TestLoadAppliesDefaults(t *testing.T) {
	cfg, err := Load[testConfig]()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Service.Environment != "development" || cfg.HTTP.Addr != ":8080" || cfg.HTTP.AdminAddr != ":9090" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.HTTP.ShutdownTimeout != 20*time.Second || cfg.HTTP.DrainDelay != 5*time.Second || !cfg.HTTP.AccessLogSuccess {
		t.Fatalf("unexpected HTTP defaults: %+v", cfg.HTTP)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	cases := []struct{ key, value, mention string }{
		{"ENVIRONMENT", "prod", "ENVIRONMENT"},
		{"LOG_FORMAT", "xml", "LOG_FORMAT"},
		{"LOG_LEVEL", "loud", "LOG_LEVEL"},
		{"HTTP_READ_TIMEOUT", "0s", "HTTP_READ_TIMEOUT"},
		{"SHUTDOWN_DRAIN_DELAY", "30s", "SHUTDOWN_DRAIN_DELAY"},
		{"HTTP_MAX_BODY_BYTES", "-1", "HTTP_MAX_BODY_BYTES"},
		{"HTTP_WRITE_TIMEOUT", "fast", "HTTP_WRITE_TIMEOUT"},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := Load[testConfig](); err == nil || !strings.Contains(err.Error(), tc.mention) {
				t.Fatalf("%s=%s: error %v should mention %s", tc.key, tc.value, err, tc.mention)
			}
		})
	}
}

type pgOnly struct{ Postgres Postgres }

func TestSecretsAreUnsetAfterLoading(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://u:p@localhost:5432/db")
	cfg, err := Load[pgOnly]()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Postgres.DSN == "" {
		t.Fatal("DSN not loaded")
	}
	if _, stillSet := os.LookupEnv("POSTGRES_DSN"); stillSet {
		t.Fatal("POSTGRES_DSN must be removed from the environment so child processes never inherit it")
	}
}

func TestRequiredSettingsAreEnforced(t *testing.T) {
	if _, set := os.LookupEnv("POSTGRES_DSN"); set {
		t.Skip("POSTGRES_DSN is set in this environment")
	}
	if _, err := Load[pgOnly](); err == nil || !strings.Contains(err.Error(), "POSTGRES_DSN") {
		t.Fatalf("missing required DSN: got %v", err)
	}
}

func TestMalformedValuesAreReportedByVariableName(t *testing.T) {
	t.Setenv("HTTP_READ_TIMEOUT", "soon")
	t.Setenv("HTTP_ACCESS_LOG_SUCCESS", "maybe")
	_, err := Load[testConfig]()
	if err == nil {
		t.Fatal("want an error")
	}
	for _, want := range []string{`HTTP_READ_TIMEOUT="soon"`, `HTTP_ACCESS_LOG_SUCCESS="maybe"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q should mention %s", err, want)
		}
	}
}
