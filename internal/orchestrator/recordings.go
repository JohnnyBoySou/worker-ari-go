package orchestrator

import (
	"context"
	"time"

	"github.com/lai/worker-ari/internal/ari"
	"github.com/lai/worker-ari/internal/logx"
	"github.com/lai/worker-ari/internal/store"
)

// uploadRecording baixa a gravação do Asterisk (via ARI) e sobe para o S3,
// gravando a key no registro da chamada.
//
// Devolve true se a gravação subiu E a key foi PERSISTIDA. Os dois: um arquivo
// no bucket com `recording_url` NULL é indistinguível de "não subiu" para o
// player, e era exatamente assim que o incidente se manifestava.
func (c *Orchestrator) uploadRecording(ctx context.Context, callID, org, name string) (bool, error) {
	if c.up == nil {
		return false, nil
	}
	bucket := c.up.BucketName()
	key := "recordings/" + org + "/" + callID + ".wav"
	logx.Info("rec.upload_begin", "callId", callID, "recording", name, "bucket", bucket, "key", key)

	// 1) Baixa o WAV do Asterisk. Ele finaliza o arquivo só DEPOIS que a bridge
	//    cai — em gravações longas / servidor ocupado isso passa de 1,5s (a janela
	//    antiga era 3x500ms, apertada demais, e a raiz do "gravação intermitente
	//    não chegou no bucket"). Backoff exponencial capado em 4s, e CADA
	//    tentativa é logada: "não chegou no bucket" deixa de ser um mistério.
	var audio []byte
	var lastErr error
	attempts := c.o.RecordingDownloadAttempts
	if attempts <= 0 {
		attempts = 8
	}
	for i := 0; i < attempts && audio == nil; i++ {
		a, err := c.ari.GetStoredRecording(ctx, name)
		if err == nil {
			audio = a
			break
		}
		lastErr = err
		wait := time.Duration(500*(1<<i)) * time.Millisecond
		if wait > 4*time.Second {
			wait = 4 * time.Second
		}
		status := 0
		var he *ari.HTTPError
		if ok := asAriHTTP(err, &he); ok {
			status = he.Status
		}
		logx.Warn("rec.download_retry", "callId", callID, "recording", name,
			"attempt", i+1, "of", attempts, "status", status,
			"waitMs", wait.Milliseconds(), "error", err.Error())
		if i < attempts-1 {
			c.o.Sleep(ctx, wait)
		}
	}
	if audio == nil {
		// Fim da janela sem o arquivo: o WAV pode existir no spool do Asterisk mas
		// o ARI não o entregou. NÃO apagamos o spool — fica lá para recuperação.
		msg := ""
		if lastErr != nil {
			msg = lastErr.Error()
		}
		logx.Error("rec.unavailable", "callId", callID, "recording", name,
			"attempts", attempts, "lastError", msg)
		return false, lastErr
	}
	logx.Info("rec.downloaded", "callId", callID, "recording", name, "bytes", len(audio))

	// 2) Sobe pro bucket. Se falhar, NÃO apaga o spool (preserva a fonte para
	//    reprocessar) e para aqui — o erro fica explícito no log.
	if err := c.up.Upload(ctx, key, audio, "audio/wav"); err != nil {
		logx.Error("rec.upload_failed", "callId", callID, "key", key, "bucket", bucket,
			"bytes", len(audio), "error", err.Error())
		return false, err
	}
	logx.Info("rec.uploaded", "callId", callID, "bucket", bucket, "key", key, "bytes", len(audio))

	// 3) Grava a key no banco. Antes isto era engolido: se o UPDATE falhasse, o
	//    arquivo ficava no bucket mas `recording_url` NULL — o player nunca achava
	//    e parecia que "não subiu". O retorno reflete a PERSISTÊNCIA, não o upload:
	//    o backfill só re-emite COMPLETED se a key de fato ficou gravada.
	saved := false
	if err := c.st.UpdateCall(ctx, callID, store.Patch{RecordingURL: store.S(key)}); err != nil {
		logx.Error("rec.db_save_failed", "callId", callID, "key", key, "error", err.Error())
	} else {
		saved = true
		logx.Info("rec.db_saved", "callId", callID, "key", key)
	}

	// 4) Remove do spool do Asterisk (o objeto já está no bucket). Best-effort.
	if err := c.ari.DeleteStoredRecording(ctx, name); err != nil {
		logx.Warn("rec.spool_delete_failed", "callId", callID, "recording", name, "error", err.Error())
	} else {
		logx.Info("rec.spool_deleted", "callId", callID, "recording", name)
	}
	return saved, nil
}

func asAriHTTP(err error, out **ari.HTTPError) bool {
	for err != nil {
		if he, ok := err.(*ari.HTTPError); ok {
			*out = he
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

type BackfillResult struct {
	Total    int `json:"total"`
	Uploaded int `json:"uploaded"`
	Skipped  int `json:"skipped"`
}

// BackfillRecordings recupera chamadas cujo áudio ficou só no Asterisk (S3
// estava desligado): baixa, sobe e RE-PUBLICA COMPLETED para o back reenfileirar
// o sync. One-off e idempotente — ao subir, o updateCall preenche recordingUrl e
// a chamada sai da próxima varredura.
func (c *Orchestrator) BackfillRecordings(ctx context.Context, limit int) (BackfillResult, error) {
	if limit <= 0 {
		limit = 200
	}
	pend, err := c.st.FindPendingRecordings(ctx, limit)
	if err != nil {
		return BackfillResult{}, err
	}
	logx.Info("backfill.start", "total", len(pend))
	res := BackfillResult{Total: len(pend)}
	for _, p := range pend {
		ok, _ := c.uploadRecording(ctx, p.CallID, p.OrganizationID, p.RecordingName)
		if ok {
			res.Uploaded++
			// Usa o org da query: a chamada não está mais em callOrg.
			c.pub.Publish(ctx, p.OrganizationID, p.CallID, "COMPLETED")
		} else {
			res.Skipped++
		}
	}
	logx.Info("backfill.done", "total", res.Total, "uploaded", res.Uploaded, "skipped", res.Skipped)
	return res, nil
}
