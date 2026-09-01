// Package ari é o client do Asterisk REST Interface: REST para controle e
// WebSocket para eventos. Port fiel de src/ari/client.ts e src/ari/types.ts.
package ari

type Channel struct {
	ID     string `json:"id"`
	State  string `json:"state"`
	Caller struct {
		Number string `json:"number"`
		Name   string `json:"name"`
	} `json:"caller"`
	Dialplan struct {
		Context string `json:"context"`
		Exten   string `json:"exten"`
	} `json:"dialplan"`
}

type Bridge struct {
	ID       string   `json:"id"`
	Channels []string `json:"channels"`
}

// Event é o envelope comum dos eventos do Stasis. Os campos específicos de cada
// tipo ficam em structs próprias, decodificadas do payload cru.
type Event struct {
	Type        string `json:"type"`
	Application string `json:"application"`
	Timestamp   string `json:"timestamp"`
}

type StasisStart struct {
	Event
	Args    []string `json:"args"`
	Channel Channel  `json:"channel"`
}

type ChannelDestroyed struct {
	Event
	Cause    *int    `json:"cause"`
	CauseTxt string  `json:"cause_txt"`
	Channel  Channel `json:"channel"`
}

// OriginateParams espelha os parâmetros de POST /channels.
type OriginateParams struct {
	ChannelID string
	Endpoint  string
	App       string
	AppArgs   string
	CallerID  string
	Timeout   int
	// Formats é OBRIGATÓRIO fixar no AudioSocket: sem isso o Asterisk cria a
	// perna em slin192 (192 kHz) e o voice-talk recebe o áudio esticado 24x (voz
	// vira sub-50 Hz e o STT não entende nada). O valor tem de casar com o
	// CALL_SR_IN do voice-talk; CALL_SR_OUT pode ser diferente por causa da
	// assimetria do AudioSocket.
	Formats   string
	Variables map[string]string
}
