package ari

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lai/worker-ari/internal/logx"
)

type HTTPError struct {
	Status  int
	Message string
}

func (e *HTTPError) Error() string { return e.Message }

// IsStatus diz se err é um HTTPError com o status dado. Substitui o
// `err instanceof AriHttpError && err.status === 409` do TS.
func IsStatus(err error, status int) bool {
	var he *HTTPError
	if ok := asHTTPError(err, &he); ok {
		return he.Status == status
	}
	return false
}

func asHTTPError(err error, out **HTTPError) bool {
	for err != nil {
		if he, ok := err.(*HTTPError); ok {
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

type Options struct {
	BaseURL  string
	Username string
	Password string
	App      string
	// AppCheck é o intervalo da prova de vida do app Stasis. <= 0 desliga.
	AppCheck     time.Duration
	ReconnectMin time.Duration
	ReconnectMax time.Duration
	HTTPClient   *http.Client
	Now          func() time.Time
	Dialer       *websocket.Dialer // injetável para teste
}

type Client struct {
	rest     string // http(s)://host:port/ari
	wsBase   string // ws(s)://host:port/ari/events
	user     string
	pass     string
	app      string
	appCheck time.Duration
	hc       *http.Client
	now      func() time.Time

	mu      sync.Mutex
	appOkAt time.Time // última confirmação de que o app está registrado

	hmu      sync.RWMutex
	handlers map[string][]Handler

	recMin, recMax time.Duration
	dialer         *websocket.Dialer
}

func New(o Options) *Client {
	base := strings.TrimRight(o.BaseURL, "/")
	hc := o.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 15 * time.Second}
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}
	appCheck := o.AppCheck
	if appCheck == 0 {
		appCheck = 30 * time.Second
	}
	return &Client{
		recMin:   o.ReconnectMin,
		recMax:   o.ReconnectMax,
		dialer:   o.Dialer,
		rest:     base + "/ari",
		wsBase:   strings.Replace(base, "http", "ws", 1) + "/ari/events",
		user:     o.Username,
		pass:     o.Password,
		app:      o.App,
		appCheck: appCheck,
		hc:       hc,
		now:      now,
	}
}

func (c *Client) App() string { return c.app }

// AppAliveAt é o momento da última confirmação de que o app está registrado.
// Zero = nunca confirmado. Lido pelo /health.
func (c *Client) AppAliveAt() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.appOkAt
}

func (c *Client) markAppOk() {
	c.mu.Lock()
	c.appOkAt = c.now()
	c.mu.Unlock()
}

// --- REST --------------------------------------------------------------------

func (c *Client) req(ctx context.Context, method, path string, query map[string]string, body any) ([]byte, error) {
	u, err := url.Parse(c.rest + path)
	if err != nil {
		return nil, err
	}
	if len(query) > 0 {
		q := u.Query()
		for k, v := range query {
			if v != "" {
				q.Set(k, v)
			}
		}
		u.RawQuery = q.Encode()
	}
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rdr)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.user, c.pass)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	data, _ := io.ReadAll(res.Body)
	if res.StatusCode >= 400 {
		return nil, &HTTPError{
			Status:  res.StatusCode,
			Message: fmt.Sprintf("%s %s -> %d %s", method, path, res.StatusCode, string(data)),
		}
	}
	return data, nil
}

func (c *Client) Originate(ctx context.Context, p OriginateParams) error {
	q := map[string]string{
		"channelId": p.ChannelID,
		"endpoint":  p.Endpoint,
		"app":       p.App,
		"appArgs":   p.AppArgs,
		"callerId":  p.CallerID,
		"formats":   p.Formats,
	}
	if p.Timeout > 0 {
		q["timeout"] = strconv.Itoa(p.Timeout)
	}
	var body any
	if len(p.Variables) > 0 {
		body = map[string]any{"variables": p.Variables}
	}
	_, err := c.req(ctx, http.MethodPost, "/channels", q, body)
	return err
}

func (c *Client) Answer(ctx context.Context, channelID string) error {
	_, err := c.req(ctx, http.MethodPost, "/channels/"+url.PathEscape(channelID)+"/answer", nil, nil)
	return err
}

func (c *Client) Hangup(ctx context.Context, channelID string) error {
	_, err := c.req(ctx, http.MethodDelete, "/channels/"+url.PathEscape(channelID), nil, nil)
	return err
}

// MuteChannel silencia uma direção. direction="in" silencia o que o canal ENVIA
// (microfone) — ele continua OUVINDO. É o que a supervisão da IA usa.
func (c *Client) MuteChannel(ctx context.Context, channelID, direction string) error {
	_, err := c.req(ctx, http.MethodPost, "/channels/"+url.PathEscape(channelID)+"/mute",
		map[string]string{"direction": direction}, nil)
	return err
}

func (c *Client) UnmuteChannel(ctx context.Context, channelID, direction string) error {
	_, err := c.req(ctx, http.MethodDelete, "/channels/"+url.PathEscape(channelID)+"/mute",
		map[string]string{"direction": direction}, nil)
	return err
}

// GetChannelVar devolve "" quando a variável não existe ou a consulta falha —
// espelha o `.catch(() => null)` do TS, onde ausência e erro são o mesmo caso
// para quem chama.
func (c *Client) GetChannelVar(ctx context.Context, channelID, variable string) string {
	data, err := c.req(ctx, http.MethodGet, "/channels/"+url.PathEscape(channelID)+"/variable",
		map[string]string{"variable": variable}, nil)
	if err != nil {
		return ""
	}
	var out struct {
		Value string `json:"value"`
	}
	if json.Unmarshal(data, &out) != nil {
		return ""
	}
	return out.Value
}

func (c *Client) SetChannelVar(ctx context.Context, channelID, variable, value string) error {
	_, err := c.req(ctx, http.MethodPost, "/channels/"+url.PathEscape(channelID)+"/variable",
		map[string]string{"variable": variable, "value": value}, nil)
	return err
}

func (c *Client) ListChannels(ctx context.Context) ([]Channel, error) {
	data, err := c.req(ctx, http.MethodGet, "/channels", nil, nil)
	if err != nil {
		return nil, err
	}
	var out []Channel
	return out, json.Unmarshal(data, &out)
}

func (c *Client) ListBridges(ctx context.Context) ([]Bridge, error) {
	data, err := c.req(ctx, http.MethodGet, "/bridges", nil, nil)
	if err != nil {
		return nil, err
	}
	var out []Bridge
	return out, json.Unmarshal(data, &out)
}

func (c *Client) CreateBridge(ctx context.Context, bridgeID, kind string) error {
	if kind == "" {
		kind = "mixing"
	}
	_, err := c.req(ctx, http.MethodPost, "/bridges",
		map[string]string{"bridgeId": bridgeID, "type": kind}, nil)
	return err
}

func (c *Client) AddChannel(ctx context.Context, bridgeID, channelID string) error {
	_, err := c.req(ctx, http.MethodPost, "/bridges/"+url.PathEscape(bridgeID)+"/addChannel",
		map[string]string{"channel": channelID}, nil)
	return err
}

func (c *Client) DestroyBridge(ctx context.Context, bridgeID string) error {
	_, err := c.req(ctx, http.MethodDelete, "/bridges/"+url.PathEscape(bridgeID), nil, nil)
	return err
}

func (c *Client) RecordBridge(ctx context.Context, bridgeID, name, format, ifExists string) error {
	if format == "" {
		format = "wav"
	}
	if ifExists == "" {
		ifExists = "overwrite"
	}
	_, err := c.req(ctx, http.MethodPost, "/bridges/"+url.PathEscape(bridgeID)+"/record",
		map[string]string{"name": name, "format": format, "ifExists": ifExists}, nil)
	return err
}

// GetStoredRecording baixa o binário de uma gravação já finalizada.
func (c *Client) GetStoredRecording(ctx context.Context, name string) ([]byte, error) {
	return c.req(ctx, http.MethodGet, "/recordings/stored/"+url.PathEscape(name)+"/file", nil, nil)
}

func (c *Client) DeleteStoredRecording(ctx context.Context, name string) error {
	_, err := c.req(ctx, http.MethodDelete, "/recordings/stored/"+url.PathEscape(name), nil, nil)
	return err
}

// CheckApp pergunta ao Asterisk, pelo REST, se ele ainda enxerga o nosso app.
//
// INCIDENTE 18/08/2026 (3 dias de telefonia parada): o socket ficou MEIO-ABERTO.
// O Asterisk perdeu o registro do app, mas nenhum "close"/"error" chegou ao
// cliente — o worker seguiu se achando conectado. O REST continuava saudável
// (originate respondia 200), então nada acusava o problema: a perna do vendedor
// caía num Stasis sem dono, o lead NUNCA era discado e a tela ficava em
// "Chamando…" para sempre.
//
// Por isso a prova de vida NÃO pode ser o estado do socket: é ele que mente.
//
// Devolve true se o app está registrado. O 404 é a única resposta que autoriza
// reconectar — qualquer outro erro (rede, 5xx, timeout) é ambíguo (pode ser a
// checagem, não a conexão) e derrubar um socket saudável por um blip seria
// trocar um bug por outro.
func (c *Client) CheckApp(ctx context.Context) (registrado bool, conclusivo bool) {
	_, err := c.req(ctx, http.MethodGet, "/applications/"+url.PathEscape(c.app), nil, nil)
	if err == nil {
		c.markAppOk()
		return true, true
	}
	if IsStatus(err, http.StatusNotFound) {
		return false, true
	}
	logx.Warn("ari.app_check_inconclusivo", "err", err.Error())
	return false, false
}
