package orchestrator

import (
	"errors"
	"testing"

	"github.com/lai/worker-ari/internal/ari"
	"github.com/lai/worker-ari/internal/metrics"
	"github.com/lai/worker-ari/internal/store"
)

// comUploader monta o orquestrador com S3 ligado, reaproveitando os defaults
// determinísticos de orq (relógio fixo, ids sequenciais, sleep instantâneo).
func comUploader(a AriClient, s store.Store, u Uploader, o Options) *Orchestrator {
	base := orq(a, s, o)
	return New(a, s, nil, u, metrics.New(), base.o)
}

func TestUploadRetentaEnquantoOAsteriskFinalizaOArquivo(t *testing.T) {
	// O Asterisk só FINALIZA o WAV depois que a bridge cai; em gravações longas
	// isso passa de 1,5s. A janela antiga (3x500ms) era apertada demais e foi a
	// raiz do "gravação intermitente não chegou no bucket".
	a, s, u := novoAri(), novoStore(), &fakeUploader{}
	a.erroGravacao = &ari.HTTPError{Status: 404, Message: "ainda finalizando"}
	c := comUploader(a, s, u, Options{RecordingDownloadAttempts: 5})

	ok, _ := c.uploadRecording(ctx, "call_1", "org_1", "connect-call_1")

	if ok {
		t.Fatal("sem áudio não pode reportar sucesso")
	}
	if n := len(a.ops("getRecording")); n != 5 {
		t.Fatalf("tentativas = %d, queria 5", n)
	}
	// NÃO apaga o spool: o WAV pode existir no Asterisk e o ARI só não o
	// entregou. Apagar aqui destruiria a única fonte para recuperação.
	if len(a.ops("deleteRecording")) != 0 {
		t.Fatal("sem áudio, o spool do Asterisk tem que ser preservado")
	}
}

func TestUploadFelizGravaKeyELimpaOSpool(t *testing.T) {
	a, s, u := novoAri(), novoStore(), &fakeUploader{}
	a.gravacao = []byte("RIFFfake")
	c := comUploader(a, s, u, Options{})

	ok, err := c.uploadRecording(ctx, "call_1", "org_1", "connect-call_1")

	if !ok || err != nil {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if len(u.enviados) != 1 || u.enviados[0] != "recordings/org_1/call_1.wav" {
		t.Fatalf("key = %v", u.enviados)
	}
	p, _ := s.ultimoPatch()
	if p.RecordingURL == nil || *p.RecordingURL != "recordings/org_1/call_1.wav" {
		t.Fatalf("recordingUrl não gravado: %+v", p)
	}
	if len(a.ops("deleteRecording")) != 1 {
		t.Fatal("com o objeto no bucket, o spool tem que sair")
	}
}

func TestUploadFalhandoPreservaOSpool(t *testing.T) {
	// Se o upload falha, o áudio no Asterisk é a única cópia. Apagar seria perder
	// a gravação de vez.
	a, s, u := novoAri(), novoStore(), &fakeUploader{erro: errors.New("s3 fora")}
	a.gravacao = []byte("RIFFfake")
	c := comUploader(a, s, u, Options{})

	ok, _ := c.uploadRecording(ctx, "call_1", "org_1", "connect-call_1")

	if ok {
		t.Fatal("upload falhou, não pode reportar sucesso")
	}
	if len(a.ops("deleteRecording")) != 0 {
		t.Fatal("upload falhou: preservar o spool")
	}
}

func TestSucessoRefleteAPERSISTENCIANaoOUpload(t *testing.T) {
	// Um objeto no bucket com `recording_url` NULL é indistinguível de "não
	// subiu" para o player — era exatamente assim que o incidente se
	// manifestava. Por isso o retorno segue o UPDATE, não o PUT.
	a, s, u := novoAri(), novoStore(), &fakeUploader{}
	a.gravacao = []byte("RIFFfake")
	s.erroUpd = errors.New("coluna faltando")
	c := comUploader(a, s, u, Options{})

	ok, _ := c.uploadRecording(ctx, "call_1", "org_1", "connect-call_1")

	if ok {
		t.Fatal("upload subiu mas a key não persistiu: não é sucesso")
	}
	if len(u.enviados) != 1 {
		t.Fatal("o objeto foi mesmo enviado")
	}
}

func TestBackfillSoRepublicaOQuePersistiu(t *testing.T) {
	// O back reage a COMPLETED para enfileirar o sync. Re-emitir sem a key
	// gravada faria o sync falhar de novo, em looping.
	a, s, u := novoAri(), novoStore(), &fakeUploader{}
	a.gravacao = []byte("RIFFfake")
	s.pend = []store.PendingRecording{
		{CallID: "call_1", OrganizationID: "org_1", RecordingName: "connect-call_1"},
	}
	c := comUploader(a, s, u, Options{})

	res, err := c.BackfillRecordings(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 1 || res.Uploaded != 1 || res.Skipped != 0 {
		t.Fatalf("res = %+v", res)
	}
}
