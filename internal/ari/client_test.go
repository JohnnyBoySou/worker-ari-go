package ari

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Port de test/ari-ws-watchdog.test.ts.
//
// O que se guarda aqui é o incidente de 18/08/2026: o socket ficou MEIO-ABERTO,
// o Asterisk perdeu o registro do app e nenhum "close" chegou ao cliente. O REST
// seguia respondendo 200, então nada acusava — a perna do vendedor caía num
// Stasis sem dono e a tela ficava em "Chamando…" por 3 dias.

type fakeAsterisk struct {
	*httptest.Server
	appRegistrado atomic.Bool
	upgrades      atomic.Int32
	appChecks     atomic.Int32
	// vivas = conexões que o SERVIDOR ainda enxerga abertas. Incrementa no
	// upgrade, decrementa quando o laço de leitura termina (que é como o lado
	// servidor percebe o fechamento). Escrever um Ping não serve de sonda: o
	// dado entra no buffer do SO e a escrita "dá certo" mesmo com o peer fechado.
	vivas atomic.Int32

	mu    sync.Mutex
	conns []*websocket.Conn
	// filtros = cada corpo de PUT /eventFilter recebido, na ordem. Uma LISTA e
	// nao um valor: o filtro tem que ser reenviado a cada reconexao, e so
	// guardando o historico o teste consegue provar isso.
	filtros [][]string
	// filtroErro = fazer o PUT falhar, para provar que a conexao sobrevive.
	filtroErro atomic.Bool
}

func (f *fakeAsterisk) filtrosRecebidos() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]string(nil), f.filtros...)
}

func (f *fakeAsterisk) derrubarConexoes() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.conns {
		_ = c.Close()
	}
	f.conns = nil
}

func novoFakeAsterisk(t *testing.T) *fakeAsterisk {
	t.Helper()
	f := &fakeAsterisk{}
	f.appRegistrado.Store(true)
	up := websocket.Upgrader{}
	mux := http.NewServeMux()

	mux.HandleFunc("/ari/events", func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		f.upgrades.Add(1)
		f.vivas.Add(1)
		f.mu.Lock()
		f.conns = append(f.conns, c)
		f.mu.Unlock()
		// Não fecha nunca por conta própria: é justamente o socket meio-aberto.
		go func() {
			defer f.vivas.Add(-1)
			for {
				if _, _, err := c.ReadMessage(); err != nil {
					return
				}
			}
		}()
	})
	mux.HandleFunc("/ari/applications/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/eventFilter") {
			if f.filtroErro.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			var corpo struct {
				Allowed []struct {
					Type string `json:"type"`
				} `json:"allowed"`
			}
			_ = json.NewDecoder(r.Body).Decode(&corpo)
			tipos := make([]string, 0, len(corpo.Allowed))
			for _, a := range corpo.Allowed {
				tipos = append(tipos, a.Type)
			}
			f.mu.Lock()
			f.filtros = append(f.filtros, tipos)
			f.mu.Unlock()
			_, _ = w.Write([]byte(`{"name":"connect"}`))
			return
		}
		f.appChecks.Add(1)
		if !f.appRegistrado.Load() {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"name":"connect"}`))
	})
	mux.HandleFunc("/ari/channels", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	})
	f.Server = httptest.NewServer(mux)
	t.Cleanup(f.Close)
	return f
}

func (f *fakeAsterisk) enviarEvento(t *testing.T, payload any) {
	t.Helper()
	b, _ := json.Marshal(payload)
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.conns {
		_ = c.WriteMessage(websocket.TextMessage, b)
	}
}

func clienteDe(f *fakeAsterisk, appCheck time.Duration) *Client {
	return New(Options{
		BaseURL:      f.URL,
		Username:     "u",
		Password:     "p",
		App:          "connect",
		AppCheck:     appCheck,
		ReconnectMin: 10 * time.Millisecond,
		ReconnectMax: 20 * time.Millisecond,
	})
}

func esperar(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	prazo := time.Now().Add(3 * time.Second)
	for time.Now().Before(prazo) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout esperando: %s", msg)
}

func TestConectaEEmiteOpen(t *testing.T) {
	f := novoFakeAsterisk(t)
	c := clienteDe(f, 0)
	var abriu atomic.Int32
	c.On("open", func(string, []byte) { abriu.Add(1) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	esperar(t, func() bool { return abriu.Load() == 1 }, "evento open")
	if c.AppAliveAt().IsZero() {
		t.Fatal("conectar tem que carimbar appAliveAt")
	}
}

func TestEntregaEventoDoStasis(t *testing.T) {
	f := novoFakeAsterisk(t)
	c := clienteDe(f, 0)
	recebidos := make(chan []byte, 4)
	c.On("StasisStart", func(_ string, p []byte) { recebidos <- p })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	esperar(t, func() bool { return f.upgrades.Load() >= 1 }, "conexão")

	f.enviarEvento(t, map[string]any{
		"type": "StasisStart", "args": []string{"outbound", "call_1"},
		"channel": map[string]any{"id": "ch1"},
	})

	select {
	case p := <-recebidos:
		var ev StasisStart
		if err := json.Unmarshal(p, &ev); err != nil {
			t.Fatal(err)
		}
		if ev.Args[0] != "outbound" || ev.Channel.ID != "ch1" {
			t.Fatalf("evento decodificado errado: %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("evento não chegou")
	}
}

// O TESTE DO INCIDENTE.
func TestWatchdogReconectaQuandoOAppSomeSemOSocketCair(t *testing.T) {
	f := novoFakeAsterisk(t)
	c := clienteDe(f, 30*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	esperar(t, func() bool { return f.upgrades.Load() == 1 }, "primeira conexão")

	// O Asterisk larga o registro do app. O socket NÃO cai — é exatamente o
	// meio-aberto do incidente. Nenhum "close" chega; só o REST sabe a verdade.
	f.appRegistrado.Store(false)

	// A única saída é o watchdog: ele tem que derrubar o zumbi e reconectar.
	esperar(t, func() bool { return f.upgrades.Load() >= 2 }, "reconexão pelo watchdog")

	f.appRegistrado.Store(true)
	esperar(t, func() bool { return !c.AppAliveAt().IsZero() }, "carimbo restaurado")
}

// Só o 404 autoriza reconectar.
func TestErroAmbiguoNaChecagemNaoDerrubaSocketSaudavel(t *testing.T) {
	// Um 500 ou um timeout pode ser problema DA CHECAGEM, não da conexão.
	// Derrubar um socket saudável por um blip seria trocar um bug por outro:
	// reiniciar o WebSocket derruba o Stasis no meio das chamadas em curso.
	f := &fakeAsterisk{}
	up := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/ari/events", func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		f.upgrades.Add(1)
		f.vivas.Add(1)
		go func() {
			defer f.vivas.Add(-1)
			for {
				if _, _, err := c.ReadMessage(); err != nil {
					return
				}
			}
		}()
	})
	mux.HandleFunc("/ari/applications/", func(w http.ResponseWriter, r *http.Request) {
		f.appChecks.Add(1)
		w.WriteHeader(http.StatusInternalServerError) // ambíguo, não conclusivo
	})
	f.Server = httptest.NewServer(mux)
	defer f.Close()

	c := clienteDe(f, 20*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	esperar(t, func() bool { return f.upgrades.Load() == 1 }, "conexão")
	esperar(t, func() bool { return f.appChecks.Load() >= 3 }, "várias checagens ambíguas")

	if u := f.upgrades.Load(); u != 1 {
		t.Fatalf("erro ambíguo não pode reconectar; houve %d conexões", u)
	}
}

func TestUmSocketPorVez(t *testing.T) {
	// A garantia que o "selo de geração" protegia no TS. Aqui ela é estrutural:
	// o laço de Run só disca a próxima conexão depois que serve() retorna. Dois
	// sockets vivos fariam o MESMO StasisStart originar a perna duas vezes.
	f := novoFakeAsterisk(t)
	c := clienteDe(f, 20*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	go c.Run(ctx)

	defer cancel()

	esperar(t, func() bool { return f.upgrades.Load() == 1 }, "conexão")

	// Força várias reconexões seguidas pelo watchdog e vigia o pico de conexões
	// simultâneas do lado servidor. No TS este era o trabalho do selo de geração;
	// aqui é o laço sequencial de Run que garante.
	pico := int32(0)
	pararVigia := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-pararVigia:
				return
			default:
				if v := f.vivas.Load(); v > pico {
					pico = v
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	for i := 0; i < 3; i++ {
		f.appRegistrado.Store(false)
		alvo := f.upgrades.Load() + 1
		esperar(t, func() bool { return f.upgrades.Load() >= alvo }, "reconexão")
		f.appRegistrado.Store(true)
		time.Sleep(40 * time.Millisecond)
	}

	close(pararVigia)
	wg.Wait()

	if pico > 1 {
		t.Fatalf("pico de %d sockets simultâneos; só pode haver um por vez", pico)
	}
}

func TestHTTPErrorEIsStatus(t *testing.T) {
	f := novoFakeAsterisk(t)
	c := clienteDe(f, 0)
	// 409 é o caso que o startOutbound trata como "já originado" em vez de FAILED.
	err := &HTTPError{Status: 409, Message: "conflito"}
	if !IsStatus(err, 409) || IsStatus(err, 404) {
		t.Fatal("IsStatus não distingue o status")
	}
	// Variável inexistente devolve "" em vez de erro — ausência e falha são o
	// mesmo caso para quem chama.
	if v := c.GetChannelVar(context.Background(), "ch_inexistente", "CALL_ID"); v != "" {
		t.Fatalf("queria vazio, veio %q", v)
	}
	if !strings.HasPrefix(c.App(), "connect") {
		t.Fatal("App() devia devolver o nome do app")
	}
}

// --- filtro de eventos -------------------------------------------------------
//
// O que estes testes guardam e o achado de 02/09/2026 (issue #2): sem filtro, o
// Asterisk serializa TODO evento do sistema no taskprocessor do app — 79.228
// eventos para 600 chamadas, das quais o worker usa 4 cada — e a fila chegou a
// 4.455 contra a marca d'agua de 500 do proprio Asterisk. O joelho de capacidade
// era essa fila, nao CPU.

func TestFiltroDeEventosDeclaraSoOQueTemHandler(t *testing.T) {
	f := novoFakeAsterisk(t)
	c := clienteDe(f, 0)
	// Ordem de registro embaralhada de proposito: o filtro tem que sair
	// ordenado, senao o corpo enviado muda a cada boot sem nada mudar de fato.
	c.On("StasisStart", func(string, []byte) {})
	c.On("open", func(string, []byte) {})
	c.On("ChannelDestroyed", func(string, []byte) {})
	c.On("close", func(string, []byte) {})

	ctx, cancelar := context.WithCancel(context.Background())
	defer cancelar()
	go c.Run(ctx)

	esperar(t, func() bool { return len(f.filtrosRecebidos()) > 0 }, "filtro enviado ao conectar")

	got := f.filtrosRecebidos()[0]
	want := []string{"ChannelDestroyed", "StasisStart"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("filtro enviado = %v, queria %v", got, want)
	}
	// "open" e "close" sao emitidos pelo PROPRIO cliente. Mandados no filtro, o
	// Asterisk nao os reconhece e a lista inteira vira recusa.
	for _, tipo := range got {
		if tipo == "open" || tipo == "close" {
			t.Fatalf("evento sintetico %q vazou para o filtro", tipo)
		}
	}
}

func TestFiltroDeEventosEReenviadoACadaReconexao(t *testing.T) {
	f := novoFakeAsterisk(t)
	c := clienteDe(f, 0)
	c.On("StasisStart", func(string, []byte) {})

	ctx, cancelar := context.WithCancel(context.Background())
	defer cancelar()
	go c.Run(ctx)

	esperar(t, func() bool { return len(f.filtrosRecebidos()) >= 1 }, "primeiro filtro")

	// O filtro vive no REGISTRO do app, e o app se registra ao abrir o socket:
	// caiu o socket, o filtro foi junto. Sem reenviar, a reconexao volta a
	// receber o sistema inteiro — e nada no worker acusaria.
	f.derrubarConexoes()

	esperar(t, func() bool { return len(f.filtrosRecebidos()) >= 2 }, "filtro reenviado apos reconectar")

	if got := f.filtrosRecebidos()[1]; !reflect.DeepEqual(got, []string{"StasisStart"}) {
		t.Fatalf("filtro da reconexao = %v, queria [StasisStart]", got)
	}
}

func TestFalhaAoFiltrarNaoDerrubaAConexao(t *testing.T) {
	f := novoFakeAsterisk(t)
	f.filtroErro.Store(true)
	c := clienteDe(f, 0)

	recebidos := make(chan string, 1)
	c.On("StasisStart", func(tipo string, _ []byte) {
		select {
		case recebidos <- tipo:
		default:
		}
	})

	ctx, cancelar := context.WithCancel(context.Background())
	defer cancelar()
	go c.Run(ctx)

	esperar(t, func() bool { return f.vivas.Load() == 1 }, "socket conectado")
	f.enviarEvento(t, map[string]any{"type": "StasisStart"})

	// Um Asterisk anterior ao 18 nao tem o endpoint: o PUT falha e ele continua
	// entregando tudo. Isso e o comportamento ANTIGO — mais lento, nao incorreto.
	// Derrubar a conexao por causa dele trocaria lentidao por indisponibilidade.
	select {
	case <-recebidos:
	case <-time.After(3 * time.Second):
		t.Fatal("evento nao chegou: a falha no filtro derrubou a conexao")
	}
}
