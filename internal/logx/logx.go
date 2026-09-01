// Package logx é o log estruturado do worker, com o mesmo formato do worker em
// TypeScript (`{"ts","level","event",...}`) para as duas versões poderem conviver
// atrás do mesmo pipeline de log durante a migração.
package logx

import (
	"log/slog"
	"os"
)

var base = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

func kv(event string, args ...any) []any {
	return append([]any{"event", event}, args...)
}

func Info(event string, args ...any)  { base.Info("", kv(event, args...)...) }
func Warn(event string, args ...any)  { base.Warn("", kv(event, args...)...) }
func Error(event string, args ...any) { base.Error("", kv(event, args...)...) }

// SetOutput troca o destino (usado nos testes para silenciar).
func SetOutput(h slog.Handler) { base = slog.New(h) }
