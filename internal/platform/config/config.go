// Package config loads typed configuration from environment variables
// (12-factor). Each binary composes the blocks it needs into one struct and
// calls Load; validation runs before anything else starts, so a bad deploy
// fails fast with a clear message instead of misbehaving at runtime.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Validator is implemented by config blocks that can check themselves.
type Validator interface {
	Validate() error
}

// Load parses environment variables into T and validates the result.
func Load[T any]() (T, error) {
	var zero T
	// Report malformed values by environment variable name. The env library
	// names the Go field instead ("ReadTimeout"), which is ambiguous when
	// several blocks share a field name and is not what an operator sets.
	if errs := checkValues(reflect.TypeOf(zero), ""); len(errs) > 0 {
		return zero, fmt.Errorf("config: %w", errors.Join(errs...))
	}
	cfg, err := env.ParseAs[T]()
	if err != nil {
		return cfg, fmt.Errorf("config: %w", err)
	}
	if v, ok := any(&cfg).(Validator); ok {
		if err := v.Validate(); err != nil {
			return cfg, fmt.Errorf("config: %w", err)
		}
	}
	return cfg, nil
}

var durationType = reflect.TypeOf(time.Duration(0))

// checkValues walks a config struct and tries to parse every set variable
// whose field is a duration, integer or boolean. Values of fields marked
// "unset" (secrets) are never echoed back.
func checkValues(t reflect.Type, prefix string) []error {
	if t == nil {
		return nil
	}
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	var errs []error
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag, hasTag := f.Tag.Lookup("env")
		if !hasTag {
			if f.Type.Kind() == reflect.Struct && f.Type != reflect.TypeOf(time.Time{}) {
				errs = append(errs, checkValues(f.Type, prefix+f.Tag.Get("envPrefix"))...)
			}
			continue
		}
		name, opts, _ := strings.Cut(tag, ",")
		if name == "" {
			continue
		}
		key := prefix + name
		raw, set := os.LookupEnv(key)
		if !set {
			continue
		}
		var err error
		switch {
		case f.Type == durationType:
			_, err = time.ParseDuration(raw)
		case f.Type.Kind() >= reflect.Int && f.Type.Kind() <= reflect.Int64:
			_, err = strconv.ParseInt(raw, 10, f.Type.Bits())
		case f.Type.Kind() >= reflect.Uint && f.Type.Kind() <= reflect.Uint64:
			_, err = strconv.ParseUint(raw, 10, f.Type.Bits())
		case f.Type.Kind() == reflect.Bool:
			_, err = strconv.ParseBool(raw)
		}
		if err == nil {
			continue
		}
		if strings.Contains(opts, "unset") {
			errs = append(errs, fmt.Errorf("%s has an invalid value for type %s", key, f.Type))
		} else {
			errs = append(errs, fmt.Errorf("%s=%q is not a valid %s", key, raw, f.Type))
		}
	}
	return errs
}

// ValidateAll runs every validator and joins their errors.
func ValidateAll(vs ...Validator) error {
	var errs []error
	for _, v := range vs {
		if err := v.Validate(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Service holds settings every HoldFast process shares.
type Service struct {
	Environment string `env:"ENVIRONMENT" envDefault:"development"`
	LogLevel    string `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat   string `env:"LOG_FORMAT" envDefault:"json"`
}

// Validate checks the shared settings.
func (s Service) Validate() error {
	switch s.Environment {
	case "development", "test", "staging", "production":
	default:
		return fmt.Errorf("ENVIRONMENT must be development, test, staging or production (got %q)", s.Environment)
	}
	switch s.LogFormat {
	case "json", "text":
	default:
		return fmt.Errorf("LOG_FORMAT must be json or text (got %q)", s.LogFormat)
	}
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(s.LogLevel)); err != nil {
		return fmt.Errorf("LOG_LEVEL: %w", err)
	}
	return nil
}

// IsProduction reports whether the process runs in production.
func (s Service) IsProduction() bool { return s.Environment == "production" }

// HTTP configures the public and internal (admin) HTTP servers.
type HTTP struct {
	Addr              string        `env:"HTTP_ADDR" envDefault:":8080"`
	AdminAddr         string        `env:"ADMIN_ADDR" envDefault:":9090"`
	ReadHeaderTimeout time.Duration `env:"HTTP_READ_HEADER_TIMEOUT" envDefault:"5s"`
	ReadTimeout       time.Duration `env:"HTTP_READ_TIMEOUT" envDefault:"10s"`
	WriteTimeout      time.Duration `env:"HTTP_WRITE_TIMEOUT" envDefault:"15s"`
	IdleTimeout       time.Duration `env:"HTTP_IDLE_TIMEOUT" envDefault:"60s"`
	RequestTimeout    time.Duration `env:"HTTP_REQUEST_TIMEOUT" envDefault:"5s"`
	MaxBodyBytes      int64         `env:"HTTP_MAX_BODY_BYTES" envDefault:"65536"`
	AccessLogSuccess  bool          `env:"HTTP_ACCESS_LOG_SUCCESS" envDefault:"true"`
	ShutdownTimeout   time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"20s"`
	DrainDelay        time.Duration `env:"SHUTDOWN_DRAIN_DELAY" envDefault:"5s"`
}

// Validate checks HTTP settings.
func (h HTTP) Validate() error {
	var errs []error
	for name, d := range map[string]time.Duration{
		"HTTP_READ_HEADER_TIMEOUT": h.ReadHeaderTimeout,
		"HTTP_READ_TIMEOUT":        h.ReadTimeout,
		"HTTP_WRITE_TIMEOUT":       h.WriteTimeout,
		"HTTP_IDLE_TIMEOUT":        h.IdleTimeout,
		"HTTP_REQUEST_TIMEOUT":     h.RequestTimeout,
		"SHUTDOWN_TIMEOUT":         h.ShutdownTimeout,
	} {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive", name))
		}
	}
	if h.DrainDelay < 0 || h.DrainDelay >= h.ShutdownTimeout {
		errs = append(errs, errors.New("SHUTDOWN_DRAIN_DELAY must be non-negative and shorter than SHUTDOWN_TIMEOUT"))
	}
	if h.MaxBodyBytes <= 0 {
		errs = append(errs, errors.New("HTTP_MAX_BODY_BYTES must be positive"))
	}
	return errors.Join(errs...)
}

// Postgres configures the connection pool.
type Postgres struct {
	DSN              string        `env:"POSTGRES_DSN,required,unset"`
	MaxConns         int32         `env:"POSTGRES_MAX_CONNS" envDefault:"20"`
	MinConns         int32         `env:"POSTGRES_MIN_CONNS" envDefault:"2"`
	MaxConnLifetime  time.Duration `env:"POSTGRES_MAX_CONN_LIFETIME" envDefault:"30m"`
	MaxConnIdleTime  time.Duration `env:"POSTGRES_MAX_CONN_IDLE_TIME" envDefault:"5m"`
	StatementTimeout time.Duration `env:"POSTGRES_STATEMENT_TIMEOUT" envDefault:"5s"`
}

// Validate checks pool settings.
func (p Postgres) Validate() error {
	if p.MaxConns < 1 || p.MinConns < 0 || p.MinConns > p.MaxConns {
		return errors.New("POSTGRES_MIN_CONNS/POSTGRES_MAX_CONNS must satisfy 0 <= min <= max and max >= 1")
	}
	if p.StatementTimeout <= 0 {
		return errors.New("POSTGRES_STATEMENT_TIMEOUT must be positive")
	}
	return nil
}

// Kafka configures the Kafka client (internal/platform/kafka).
type Kafka struct {
	Brokers     []string      `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"localhost:29092"`
	DialTimeout time.Duration `env:"KAFKA_DIAL_TIMEOUT" envDefault:"5s"`
	// DeliveryTimeout bounds how long a produced record may take to be
	// acknowledged, retries included.
	DeliveryTimeout time.Duration `env:"KAFKA_DELIVERY_TIMEOUT" envDefault:"30s"`
}

// Validate checks client settings.
func (k Kafka) Validate() error {
	if len(k.Brokers) == 0 {
		return errors.New("KAFKA_BROKERS must list at least one broker")
	}
	if k.DialTimeout <= 0 || k.DeliveryTimeout <= 0 {
		return errors.New("KAFKA_DIAL_TIMEOUT and KAFKA_DELIVERY_TIMEOUT must be positive")
	}
	return nil
}

// Valkey configures the Valkey/Redis client. One address means a standalone
// node; several addresses mean Cluster; VALKEY_SENTINEL_MASTER enables Sentinel.
type Valkey struct {
	Addrs        []string      `env:"VALKEY_ADDRS" envSeparator:"," envDefault:"localhost:6379"`
	Username     string        `env:"VALKEY_USERNAME"`
	Password     string        `env:"VALKEY_PASSWORD,unset"`
	DB           int           `env:"VALKEY_DB" envDefault:"0"`
	MasterName   string        `env:"VALKEY_SENTINEL_MASTER"`
	PoolSize     int           `env:"VALKEY_POOL_SIZE" envDefault:"64"`
	DialTimeout  time.Duration `env:"VALKEY_DIAL_TIMEOUT" envDefault:"2s"`
	ReadTimeout  time.Duration `env:"VALKEY_READ_TIMEOUT" envDefault:"500ms"`
	WriteTimeout time.Duration `env:"VALKEY_WRITE_TIMEOUT" envDefault:"500ms"`
}

// Validate checks client settings.
func (v Valkey) Validate() error {
	if len(v.Addrs) == 0 {
		return errors.New("VALKEY_ADDRS must list at least one address")
	}
	if v.PoolSize < 1 {
		return errors.New("VALKEY_POOL_SIZE must be at least 1")
	}
	return nil
}
