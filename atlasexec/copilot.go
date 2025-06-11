package atlasexec

import (
	"context"
	"strings"
)

type (
	CopilotParams struct{ Prompt, Session string }
	// Copilot is the result of a Copilot execution.
	Copilot []*CopilotMessage
	// CopilotMessage is the JSON message emitted by the Copilot OneShot execution.
	CopilotMessage struct {
		// Session ID for the Copilot session.
		SessionID string `json:"sessionID,omitempty"`
		// Type of the message. Only "message" is currently supported.
		Type string `json:"type"`
		// Content of the message,
		Content string `json:"content,omitempty"`
	}
)

// Copilot executes a one-shot Copilot session with the provided options.
func (c *Client) Copilot(ctx context.Context, params *CopilotParams) (Copilot, error) {
	args := []string{"copilot", "-q", params.Prompt}
	if params.Session != "" {
		args = append(args, "-r", params.Session)
	}
	return jsonDecode[CopilotMessage](c.runCommand(ctx, args))
}

func (c Copilot) String() string {
	var buf strings.Builder
	for _, msg := range c {
		buf.WriteString(msg.Content)
	}
	return buf.String()
}
