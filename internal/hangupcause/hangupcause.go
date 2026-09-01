// Package hangupcause classifica a causa Q.850 da perna que ENCERROU a chamada
// sem que ela fosse atendida, em (status final, failureReason estável).
//
// Port fiel de src/hangup-cause.ts. O objetivo é o front distinguir "culpa do
// provedor VoIP" de "culpa do cliente" (ocupado/não atendeu) e de "nossa infra"
// (originate_error etc., setados em outros pontos).
//
// A causa chega no evento ARI ChannelDestroyed (`cause`/`cause_txt`) — é o
// código Q.850 que o Asterisk deriva da resposta SIP do carrier (ex.: 503
// "No Circuit/Channel Available" → cause 34).
//
// IMPORTANTE: só classificar quando a chamada NÃO foi atendida. Se atendeu
// (startedAt != nil), o desfecho é COMPLETED independentemente da causa da queda
// (o normal clearing 16 no fim de uma conversa não é falha).
package hangupcause

type Classification struct {
	Status string // "FAILED" | "NO_ANSWER"
	// Código estável que o front mapeia para o rótulo pt-br (CALL_FAILURE_LABELS
	// no front). Vazio quando não há causa reconhecida (encerramento comum /
	// cancelamento) — aí fica NO_ANSWER "puro".
	FailureReason string
}

// Classify traduz a causa Q.850. `cause == nil` = terminate manual / sem causa.
//
// Causas de provedor/rede viram FAILED (tom "danger", separadas do NO_ANSWER
// neutro); ocupado/rejeitado/sem resposta são do cliente e ficam em NO_ANSWER.
func Classify(cause *int) Classification {
	if cause == nil {
		return Classification{Status: "NO_ANSWER"}
	}
	switch *cause {
	// --- Provedor / rede (VoIP) ---
	case 34, // No circuit/channel available (congestionamento)
		42: // Switching equipment congestion
		return Classification{Status: "FAILED", FailureReason: "carrier_congestion"}
	case 27, // Destination out of order
		38, // Network out of order
		41, // Temporary failure
		47, // Resource unavailable, unspecified
		63: // Service or option not available
		return Classification{Status: "FAILED", FailureReason: "carrier_unavailable"}

	// --- Dado (número) ---
	case 1, // Unallocated (unassigned) number
		3,  // No route to destination
		22, // Number changed
		28: // Invalid number format / incomplete
		return Classification{Status: "FAILED", FailureReason: "invalid_number"}

	// --- Cliente (destino) ---
	case 17: // User busy
		return Classification{Status: "NO_ANSWER", FailureReason: "busy"}
	case 21: // Call rejected
		return Classification{Status: "NO_ANSWER", FailureReason: "rejected"}
	case 16, // Normal clearing (sem atender: desligou no toque)
		18, // No user responding
		19, // No answer (user alerted)
		20: // Subscriber absent
		return Classification{Status: "NO_ANSWER", FailureReason: "no_answer"}

	// --- Sem causa reconhecida: NÃO inventa falha — NO_ANSWER puro. A causa
	// crua ainda é guardada (hangup_cause/txt) para o suporte investigar.
	default:
		return Classification{Status: "NO_ANSWER"}
	}
}
