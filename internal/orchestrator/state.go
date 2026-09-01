package orchestrator

import (
	"sync"
	"time"
)

// bridgeRef é o estado em memória de uma chamada em curso.
type bridgeRef struct {
	bridgeID        string
	sellerChannelID string
	leadChannelID   string
	// agentChannelID: supervisão da IA — a perna WebRTC do agente que ouve a
	// IA↔lead (mutada "in") e, no takeover, é desmutada para assumir.
	agentChannelID string
	// aiMediaUUID: o mesmo do endpoint AudioSocket/addr/<uuid>, que o voice-talk
	// usa como chave da sessão para o handoff. Vazio fora do fluxo IA.
	aiMediaUUID string
	// handingOff: true enquanto o takeover está em curso. Quando a perna da IA
	// cair, desmutamos o agente em vez de encerrar a chamada.
	handingOff    bool
	startedAt     *time.Time
	recordingName string
}

func (r *bridgeRef) clone() bridgeRef { return *r }

// state guarda TODO o estado mutável do orquestrador atrás de um mutex.
//
// POR QUE O MUTEX EXISTE — e por que ele não estava no original.
//
// O worker em TypeScript é single-threaded: o event loop serializa os handlers, e
// os `Map` só podiam ser observados entre `await`s. Em Go, o consumidor de
// comandos e os eventos do ARI rodam em goroutines CONCORRENTES, e acesso
// concorrente a map em Go não é um bug sutil — é panic do runtime.
//
// A REGRA DO PORT, aplicada mecanicamente em todo este pacote:
//
//	cada bloco contíguo de operações de mapa que no TS NÃO tem `await` entre
//	elas vira UMA única seção crítica aqui.
//
// É por isso que existe `withRef` (aplica uma mutação atômica sobre o ref vivo) e
// por que os getters devolvem CÓPIA: se o chamador guardasse o ponteiro e o
// mutasse fora do lock depois de um await, teríamos a corrida de volta,
// exatamente onde ela é mais cara.
type state struct {
	mu sync.Mutex
	// callId -> ref da bridge.
	bridges map[string]*bridgeRef
	// callId -> organizationId, para escolher o canal pub/sub em cada transição.
	// Seu TAMANHO é o gauge de chamadas ativas (sem bookkeeping duplo).
	callOrg map[string]string
	// channelId -> callId: índice reverso das pernas. Torna o onChannelDestroyed
	// O(1) em vez de varrer `bridges`. Espelha EXATAMENTE o ciclo de vida de
	// `bridges` — um delete esquecido aqui vaza entradas mortas.
	channelIndex map[string]string
	// callId -> ramal WebRTC do agente supervisor (ligação IA supervisionada).
	aiAgent map[string]string
	// callId -> UUID do AudioSocket pré-escolhido pela API.
	aiVoiceUUID map[string]string
}

func newState() *state {
	return &state{
		bridges:      map[string]*bridgeRef{},
		callOrg:      map[string]string{},
		channelIndex: map[string]string{},
		aiAgent:      map[string]string{},
		aiVoiceUUID:  map[string]string{},
	}
}

func (s *state) activeCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.callOrg)
}

func (s *state) setOrg(callID, org string) {
	s.mu.Lock()
	s.callOrg[callID] = org
	s.mu.Unlock()
}

func (s *state) org(callID string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.callOrg[callID]
	return v, ok
}

func (s *state) hasOrg(callID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.callOrg[callID]
	return ok
}

func (s *state) deleteOrg(callID string) {
	s.mu.Lock()
	delete(s.callOrg, callID)
	s.mu.Unlock()
}

// limparIA é o bloco final do finalize: org + agente + uuid pré-escolhido.
// No TS são três deletes seguidos sem await, então aqui é uma seção crítica só.
func (s *state) limparIA(callID string) {
	s.mu.Lock()
	delete(s.callOrg, callID)
	delete(s.aiAgent, callID)
	delete(s.aiVoiceUUID, callID)
	s.mu.Unlock()
}

func (s *state) setAIAgent(callID, ramal string) {
	s.mu.Lock()
	s.aiAgent[callID] = ramal
	s.mu.Unlock()
}

func (s *state) aiAgentDe(callID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.aiAgent[callID]
}

func (s *state) setAIVoiceUUID(callID, uuid string) {
	s.mu.Lock()
	s.aiVoiceUUID[callID] = uuid
	s.mu.Unlock()
}

// consumirAIVoiceUUID lê e apaga numa seção só (no TS é get seguido de delete
// sem await entre eles).
func (s *state) consumirAIVoiceUUID(callID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.aiVoiceUUID[callID]
	delete(s.aiVoiceUUID, callID)
	return v
}

// indexar/desindexar assumem o lock JÁ tomado.
func (s *state) indexarLocked(callID string, r *bridgeRef) {
	for _, c := range []string{r.sellerChannelID, r.leadChannelID, r.agentChannelID} {
		if c != "" {
			s.channelIndex[c] = callID
		}
	}
}

func (s *state) desindexarLocked(r *bridgeRef) {
	for _, c := range []string{r.sellerChannelID, r.leadChannelID, r.agentChannelID} {
		if c != "" {
			delete(s.channelIndex, c)
		}
	}
}

// setBridge grava o ref e indexa as pernas — no TS é `bridges.set` + `indexBridge`
// coladas, então é uma seção crítica só.
func (s *state) setBridge(callID string, r bridgeRef) {
	s.mu.Lock()
	ref := r
	s.bridges[callID] = &ref
	s.indexarLocked(callID, &ref)
	s.mu.Unlock()
}

// bridge devolve uma CÓPIA. Ver o comentário do struct: entregar o ponteiro
// convidaria o chamador a mutá-lo depois de um await, fora do lock.
func (s *state) bridge(callID string) (bridgeRef, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.bridges[callID]
	if !ok {
		return bridgeRef{}, false
	}
	return r.clone(), true
}

// withRef aplica uma mutação ATÔMICA sobre o ref vivo. É o que preserva a
// atomicidade que o event loop do TS dava de graça.
func (s *state) withRef(callID string, fn func(r *bridgeRef, s *state)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.bridges[callID]
	if !ok {
		return false
	}
	fn(r, s)
	return true
}

// removerBridge apaga e desindexa numa seção só (TS: `bridges.delete` +
// `unindexBridge`, coladas).
func (s *state) removerBridge(callID string) (bridgeRef, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.bridges[callID]
	if !ok {
		return bridgeRef{}, false
	}
	delete(s.bridges, callID)
	s.desindexarLocked(r)
	return r.clone(), true
}

// porCanal resolve callId + ref pelo índice reverso, numa seção só.
func (s *state) porCanal(channelID string) (string, bridgeRef, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	callID, ok := s.channelIndex[channelID]
	if !ok {
		return "", bridgeRef{}, false
	}
	r, ok := s.bridges[callID]
	if !ok {
		return callID, bridgeRef{}, false
	}
	return callID, r.clone(), true
}
