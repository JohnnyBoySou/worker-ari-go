package orchestrator

import (
	"context"
	"sync"

	"github.com/lai/worker-ari/internal/logx"
)

// Rehydrate varre os canais/bridges ativos no ARI e remonta o estado de chamadas
// que o worker NÃO tem em memória (após restart).
//
// MERGE, não sobrescrita: chamadas já rastreadas são INTOCADAS — um blip de
// WebSocket não pode corromper estado vivo. Roda a cada (re)conexão.
func (c *Orchestrator) Rehydrate(ctx context.Context) {
	channels, err := c.ari.ListChannels(ctx)
	if err != nil {
		logx.Error("rehydrate.listar_canais_falhou", "err", err.Error())
		return
	}
	bridges, err := c.ari.ListBridges(ctx)
	if err != nil {
		logx.Error("rehydrate.listar_bridges_falhou", "err", err.Error())
		return
	}

	bridgeOf := map[string]string{}
	for _, b := range bridges {
		for _, ch := range b.Channels {
			bridgeOf[ch] = b.ID
		}
	}

	type resolvido struct {
		id, callID, org string
	}

	// Resolve CALL_ID + ORG de TODOS os canais em PARALELO. Em série seriam ~2N
	// round-trips: com N chamadas ativas num restart, isso é uma janela longa em
	// que eventos já chegam sem o estado remontado. A ordem de `channels` é
	// preservada pelo índice, então o agrupamento fica idêntico ao serial.
	out := make([]resolvido, len(channels))
	var wg sync.WaitGroup
	for i, ch := range channels {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			out[i] = resolvido{
				id:     id,
				callID: c.ari.GetChannelVar(ctx, id, "CALL_ID"),
				org:    c.ari.GetChannelVar(ctx, id, "ORG"),
			}
		}(i, ch.ID)
	}
	wg.Wait()

	type acc struct {
		org        string
		channelIDs []string
		bridgeID   string
	}
	byCall := map[string]*acc{}
	ordem := []string{}
	for _, ch := range out {
		// Sem CALL_ID = não rastreável. Já em callOrg = vivo, não tocar.
		if ch.callID == "" || c.s.hasOrg(ch.callID) {
			continue
		}
		a, ok := byCall[ch.callID]
		if !ok {
			a = &acc{}
			byCall[ch.callID] = a
			ordem = append(ordem, ch.callID)
		}
		a.channelIDs = append(a.channelIDs, ch.id)
		if ch.org != "" {
			a.org = ch.org
		}
		if a.bridgeID == "" {
			a.bridgeID = bridgeOf[ch.id]
		}
	}

	count := 0
	for _, callID := range ordem {
		a := byCall[callID]
		if a.org == "" {
			continue
		}
		timing, err := c.st.GetCallTiming(ctx, callID)
		if err != nil || timing == nil {
			continue
		}
		// Chamada já finalizada no banco não volta à vida.
		switch timing.Status {
		case "COMPLETED", "FAILED", "NO_ANSWER":
			continue
		}
		c.s.setOrg(callID, a.org)
		if a.bridgeID != "" {
			ref := bridgeRef{
				bridgeID:      a.bridgeID,
				startedAt:     timing.StartedAt,
				recordingName: "connect-" + callID,
			}
			if len(a.channelIDs) > 0 {
				ref.sellerChannelID = a.channelIDs[0]
			}
			if len(a.channelIDs) > 1 {
				ref.leadChannelID = a.channelIDs[1]
			}
			if len(a.channelIDs) > 2 {
				ref.agentChannelID = a.channelIDs[2]
			}
			c.s.setBridge(callID, ref)
		}
		count++
	}
	if count > 0 {
		logx.Info("rehydrate.remontadas", "chamadas", count)
	}
}
