package ari

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lai/worker-ari/internal/logx"
)

// Handler recebe o payload cru do evento, já sabendo o tipo.
type Handler func(tipo string, payload []byte)

// On registra um handler por tipo de evento. Além dos eventos do ARI
// (StasisStart, ChannelDestroyed), o client emite os sintéticos "open" e
// "close", com payload vazio.
//
// COPY-ON-WRITE. Os registros acontecem no boot e param; o `emit` roda para
// TODO evento do Asterisk — e a assinatura é `subscribeAll=true`, então chega
// muito mais do que os dois tipos que este worker trata (ChannelVarset, DTMF,
// eventos de bridge…). A leitura precisa ser o mais barata possível, e a versão
// com RWMutex copiava o slice de handlers a cada evento: uma alocação por
// evento, no caminho mais quente do processo, para um slice que nunca muda
// depois do boot. Aqui o `emit` faz um load atômico e itera o mapa vivo — zero
// alocação, zero lock — e o `On` publica um mapa NOVO.
func (c *Client) On(tipo string, h Handler) {
	c.hmu.Lock()
	defer c.hmu.Unlock()
	novo := map[string][]Handler{}
	if atual := c.handlers.Load(); atual != nil {
		for k, v := range *atual {
			novo[k] = v
		}
	}
	// append sobre uma CÓPIA do slice: um emit em curso pode estar iterando o
	// slice antigo, e crescer o mesmo array por baixo dele seria corrida.
	novo[tipo] = append(append([]Handler(nil), novo[tipo]...), h)
	c.handlers.Store(&novo)
}

func (c *Client) emit(tipo string, payload []byte) {
	m := c.handlers.Load()
	if m == nil {
		return
	}
	hs := (*m)[tipo]
	if len(hs) == 0 {
		return
	}
	for _, h := range hs {
		func() {
			// Handler de aplicação que entra em pânico NÃO pode derrubar o
			// processo: é o equivalente do try/catch por listener no TS. As
			// chamadas em curso dependem deste laço continuar vivo.
			defer func() {
				if r := recover(); r != nil {
					logx.Error("ari.handler_panic", "tipo", tipo, "panic", r)
				}
			}()
			h(tipo, payload)
		}()
	}
}

// Run mantém o WebSocket de eventos vivo até o contexto ser cancelado.
//
// TRADUÇÃO DELIBERADA. O original em TS carrega um "selo de geração"
// (`this.generation`) porque callbacks de WebSocket em JS não são canceláveis:
// um socket abandonado ainda entrega "close" e "message" atrasados, e sem o selo
// isso viraria (a) um segundo laço de reconexão em paralelo e (b) um StasisStart
// do zumbi originando de novo uma perna que já tem dono.
//
// Em Go o laço sequencial abaixo dá as MESMAS três garantias, estruturalmente:
//
//  1. só existe uma conexão sendo lida por vez — a próxima só é discada depois
//     que `serve` retorna;
//  2. a prova de vida do app é uma goroutine filha do contexto da conexão, então
//     morre junto com ela; um 404 tardio não tem como agendar nada;
//  3. a reconexão é o passo seguinte do mesmo laço, não um callback.
//
// Não há selo porque não há o que selar.
func (c *Client) Run(ctx context.Context) {
	backoff := c.reconnectMin()
	for {
		if ctx.Err() != nil {
			return
		}
		inicio := time.Now()
		c.serve(ctx)
		if ctx.Err() != nil {
			return
		}
		// Conexão que durou o bastante zera o backoff: um socket que ficou de pé
		// 5 minutos e caiu é um blip, não uma indisponibilidade — não faz sentido
		// esperar 30s para tentar de novo.
		if time.Since(inicio) > 30*time.Second {
			backoff = c.reconnectMin()
		}
		logx.Warn("ari.ws_caiu", "reconectaEmMs", backoff.Milliseconds())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > c.reconnectMax() {
			backoff = c.reconnectMax()
		}
	}
}

// serve mantém UMA conexão até ela morrer.
func (c *Client) serve(ctx connCtx) {
	connCtxV, cancelar := context.WithCancel(ctx)
	defer cancelar()

	u := c.wsBase + "?app=" + url.QueryEscape(c.app) +
		"&subscribeAll=true&api_key=" + url.QueryEscape(c.user+":"+c.pass)

	dialer := c.dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	conn, _, err := dialer.DialContext(connCtxV, u, http.Header{})
	if err != nil {
		logx.Warn("ari.ws_dial_falhou", "err", err.Error())
		return
	}
	defer func() { _ = conn.Close() }()

	c.markAppOk()
	logx.Info("ari.ws_conectado", "app", c.app)

	c.emit("open", nil)
	defer c.emit("close", nil)

	// Prova de vida: goroutine FILHA do contexto desta conexão. Quando `serve`
	// retorna, `cancelar()` a mata — é isto que substitui o selo de geração.
	if c.appCheck > 0 {
		go c.appCheckLoop(connCtxV, cancelar, conn)
	}

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var env Event
		if json.Unmarshal(data, &env) != nil || env.Type == "" {
			continue
		}
		c.emit(env.Type, data)
	}
}

type connCtx = context.Context

func (c *Client) appCheckLoop(ctx context.Context, cancelar context.CancelFunc, conn *websocket.Conn) {
	t := time.NewTicker(c.appCheck)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			registrado, conclusivo := c.CheckApp(ctx)
			if conclusivo && !registrado {
				// 404 = o Asterisk afirma que ninguém assina o app: o socket
				// está morto por dentro. Fechar o socket faz o ReadMessage
				// retornar erro e `serve` sair — a reconexão é o laço de Run,
				// não um agendamento paralelo.
				logx.Warn("ari.app_nao_registrado", "app", c.app)
				_ = conn.Close()
				cancelar()
				return
			}
		}
	}
}

func (c *Client) reconnectMin() time.Duration {
	if c.recMin > 0 {
		return c.recMin
	}
	return time.Second
}

func (c *Client) reconnectMax() time.Duration {
	if c.recMax > 0 {
		return c.recMax
	}
	return 30 * time.Second
}
