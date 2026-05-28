package proxy

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"kiro-go/config"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestNormalizeChunkBasicProgression(t *testing.T) {
	prev := ""

	if got := normalizeChunk("abc", &prev); got != "abc" {
		t.Fatalf("expected first chunk to pass through, got %q", got)
	}
	if got := normalizeChunk("abcde", &prev); got != "de" {
		t.Fatalf("expected appended delta, got %q", got)
	}
}

func TestNormalizeChunkPrefixRewindDoesNotReplay(t *testing.T) {
	prev := ""

	_ = normalizeChunk("abcde", &prev)
	if got := normalizeChunk("abc", &prev); got != "" {
		t.Fatalf("expected rewind chunk to be ignored, got %q", got)
	}
	if prev != "abcde" {
		t.Fatalf("expected previous snapshot to remain longest version, got %q", prev)
	}
	if got := normalizeChunk("abcdef", &prev); got != "f" {
		t.Fatalf("expected only unseen suffix after rewind, got %q", got)
	}
}

func TestNormalizeChunkOverlapDelta(t *testing.T) {
	prev := "hello world"

	if got := normalizeChunk("world!!!", &prev); got != "!!!" {
		t.Fatalf("expected overlap suffix delta, got %q", got)
	}
}

func TestGetContextWindowSizeTreatsOpus48AsOneMillion(t *testing.T) {
	for _, model := range []string{"claude-opus-4.8", "claude-opus-4-8"} {
		if got := getContextWindowSize(model); got != 1_000_000 {
			t.Fatalf("expected %s to use 1M context window, got %d", model, got)
		}
	}
}

func TestParseEventStreamFinishesPendingToolUseOnEOF(t *testing.T) {
	stream := bytes.NewReader(awsEventStreamFrame(t, "toolUseEvent", map[string]interface{}{
		"toolUseId": "toolu_1",
		"name":      "mcpIdaProMcpStatus",
		"input":     `{"server":"ida-pro-mcp"}`,
	}))

	var toolUses []KiroToolUse
	var completed bool
	err := parseEventStream(stream, &KiroStreamCallback{
		OnToolUse: func(toolUse KiroToolUse) {
			toolUses = append(toolUses, toolUse)
		},
		OnComplete: func(_, _ int) {
			completed = true
		},
	})
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if !completed {
		t.Fatalf("expected stream completion callback")
	}
	if len(toolUses) != 1 {
		t.Fatalf("expected pending tool use to be emitted on EOF, got %d", len(toolUses))
	}
	if toolUses[0].ToolUseID != "toolu_1" || toolUses[0].Name != "mcpIdaProMcpStatus" {
		t.Fatalf("unexpected tool use: %#v", toolUses[0])
	}
	if got := toolUses[0].Input["server"]; got != "ida-pro-mcp" {
		t.Fatalf("expected parsed tool input, got %#v", toolUses[0].Input)
	}
}

func TestParseEventStreamNilCallbackIsNoOp(t *testing.T) {
	stream := bytes.NewReader(bytes.Join([][]byte{
		awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{"content": "hello"}),
		awsEventStreamFrame(t, "reasoningContentEvent", map[string]interface{}{"text": "thinking"}),
		awsEventStreamFrame(t, "contextUsageEvent", map[string]interface{}{"contextUsagePercentage": 12.5}),
		awsEventStreamFrame(t, "meteringEvent", map[string]interface{}{"usage": 1.25}),
		awsEventStreamFrame(t, "toolUseEvent", map[string]interface{}{
			"name":  "mcpIdaProMcpStatus",
			"input": `{"server":"ida-pro-mcp"}`,
			"stop":  true,
		}),
	}, nil))

	if err := parseEventStream(stream, nil); err != nil {
		t.Fatalf("expected nil callback to be a no-op, got %v", err)
	}
}

func TestParseEventStreamNilCallbackFieldsAreNoOp(t *testing.T) {
	stream := bytes.NewReader(awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{
		"content": "hello",
	}))

	if err := parseEventStream(stream, &KiroStreamCallback{}); err != nil {
		t.Fatalf("expected empty callback to be a no-op, got %v", err)
	}
}

func TestHandleToolUseEventGeneratesMissingToolUseID(t *testing.T) {
	var toolUses []KiroToolUse
	current := handleToolUseEvent(map[string]interface{}{
		"name":  "mcpIdaProMcpStatus",
		"input": `{"server":"ida-pro-mcp"}`,
		"stop":  true,
	}, nil, &KiroStreamCallback{
		OnToolUse: func(toolUse KiroToolUse) {
			toolUses = append(toolUses, toolUse)
		},
	})

	if current != nil {
		t.Fatalf("expected stopped tool use to clear current state")
	}
	if len(toolUses) != 1 {
		t.Fatalf("expected one tool use, got %d", len(toolUses))
	}
	if toolUses[0].ToolUseID == "" {
		t.Fatalf("expected generated tool use id")
	}
	if toolUses[0].Name != "mcpIdaProMcpStatus" {
		t.Fatalf("unexpected tool name: %q", toolUses[0].Name)
	}
}

func TestHandleToolUseEventReplacesGeneratedIDWhenRealIDArrives(t *testing.T) {
	var toolUses []KiroToolUse
	callback := &KiroStreamCallback{
		OnToolUse: func(toolUse KiroToolUse) {
			toolUses = append(toolUses, toolUse)
		},
	}

	current := handleToolUseEvent(map[string]interface{}{
		"name":  "mcpIdaProMcpStatus",
		"input": `{"server":`,
	}, nil, callback)
	current = handleToolUseEvent(map[string]interface{}{
		"toolUseId": "toolu_real",
		"name":      "mcpIdaProMcpStatus",
		"input":     `"ida-pro-mcp"}`,
		"stop":      true,
	}, current, callback)

	if current != nil {
		t.Fatalf("expected stopped tool use to clear current state")
	}
	if len(toolUses) != 1 {
		t.Fatalf("expected one completed tool use, got %d", len(toolUses))
	}
	if toolUses[0].ToolUseID != "toolu_real" {
		t.Fatalf("expected real tool id to replace generated id, got %q", toolUses[0].ToolUseID)
	}
	if got := toolUses[0].Input["server"]; got != "ida-pro-mcp" {
		t.Fatalf("expected joined tool input, got %#v", toolUses[0].Input)
	}
}

func TestBuildKiroTransportUsesExplicitProxyURL(t *testing.T) {
	transport := buildKiroTransport("http://proxy.local:8080")
	req := &http.Request{URL: mustParseURL(t, "https://q.us-east-1.amazonaws.com")}

	got, err := transport.Proxy(req)
	if err != nil {
		t.Fatalf("unexpected proxy error: %v", err)
	}
	assertProxyURL(t, got, "http://proxy.local:8080")
}

func TestBuildKiroTransportFallsBackToEnvironmentProxy(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://env-proxy.local:2323")
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")

	transport := buildKiroTransport("")
	req := &http.Request{URL: mustParseURL(t, "https://q.us-east-1.amazonaws.com")}

	got, err := transport.Proxy(req)
	if err != nil {
		t.Fatalf("unexpected proxy error: %v", err)
	}
	assertProxyURL(t, got, "http://env-proxy.local:2323")
}

func TestInitKiroHttpClientKeepsShortRestTimeout(t *testing.T) {
	InitKiroHttpClient("")
	t.Cleanup(func() { InitKiroHttpClient("") })

	streamClient := kiroHttpStore.Load()
	restClient := kiroRestHttpStore.Load()

	if streamClient.Timeout != kiroStreamingTimeout {
		t.Fatalf("expected streaming timeout to be %s, got %s", kiroStreamingTimeout, streamClient.Timeout)
	}
	if restClient.Timeout != 30*time.Second {
		t.Fatalf("expected REST timeout to stay 30s, got %s", restClient.Timeout)
	}
}

func TestGetClientForProxyUsesLongStreamTimeout(t *testing.T) {
	client := GetClientForProxy("http://test-proxy-zero-timeout:1234")
	if client.Timeout != kiroStreamingTimeout {
		t.Fatalf("expected per-proxy streaming client timeout to be %s, got %s", kiroStreamingTimeout, client.Timeout)
	}
}

func TestGetRestClientForProxyHas30sTimeout(t *testing.T) {
	client := GetRestClientForProxy("http://test-rest-proxy-timeout:5678")
	if client.Timeout != 30*time.Second {
		t.Fatalf("expected per-proxy REST client timeout to be 30s, got %s", client.Timeout)
	}
}

func TestBuildKiroTransportUsesConnectionLevelTimeouts(t *testing.T) {
	transport := buildKiroTransport("")
	if transport.DialContext == nil {
		t.Fatal("expected DialContext to be configured")
	}
	if transport.TLSHandshakeTimeout != 10*time.Second {
		t.Fatalf("expected TLS handshake timeout to be 10s, got %s", transport.TLSHandshakeTimeout)
	}
	if transport.ResponseHeaderTimeout != 60*time.Second {
		t.Fatalf("expected response header timeout to be 60s, got %s", transport.ResponseHeaderTimeout)
	}
	if transport.ExpectContinueTimeout != time.Second {
		t.Fatalf("expected expect-continue timeout to be 1s, got %s", transport.ExpectContinueTimeout)
	}
}

// ==================== EventStream Header Parsing Tests ====================

func TestExtractEventStreamHeadersEventTypeOnly(t *testing.T) {
	headers := buildEventStreamHeaderString(":event-type", "assistantResponseEvent")
	result := extractEventStreamHeaders(headers)
	if result.EventType != "assistantResponseEvent" {
		t.Fatalf("expected EventType=assistantResponseEvent, got %q", result.EventType)
	}
	if result.MessageType != "" {
		t.Fatalf("expected MessageType empty, got %q", result.MessageType)
	}
}

func TestExtractEventStreamHeadersAllFields(t *testing.T) {
	var buf []byte
	buf = append(buf, buildEventStreamHeaderString(":event-type", "exception")...)
	buf = append(buf, buildEventStreamHeaderString(":message-type", "exception")...)
	buf = append(buf, buildEventStreamHeaderString(":exception-type", "ThrottlingException")...)
	buf = append(buf, buildEventStreamHeaderString(":error-code", "429")...)

	result := extractEventStreamHeaders(buf)
	if result.EventType != "exception" {
		t.Fatalf("expected EventType=exception, got %q", result.EventType)
	}
	if result.MessageType != "exception" {
		t.Fatalf("expected MessageType=exception, got %q", result.MessageType)
	}
	if result.ExceptionType != "ThrottlingException" {
		t.Fatalf("expected ExceptionType=ThrottlingException, got %q", result.ExceptionType)
	}
	if result.ErrorCode != "429" {
		t.Fatalf("expected ErrorCode=429, got %q", result.ErrorCode)
	}
}

// ==================== parseEventStream Exception/Error Frame Tests ====================

func TestParseEventStreamExceptionFrame(t *testing.T) {
	payload := []byte(`{"message":"rate limited"}`)
	frame := buildEventStreamFrame(t, map[string]string{
		":event-type":     "exception",
		":message-type":   "exception",
		":exception-type": "ThrottlingException",
	}, payload)

	var onCompleteCalled bool
	callback := &KiroStreamCallback{
		OnComplete: func(inTok, outTok int) {
			onCompleteCalled = true
		},
	}

	err := parseEventStream(bytes.NewReader(frame), callback)
	if err == nil {
		t.Fatal("expected error from exception frame, got nil")
	}
	errMsg := err.Error()
	if !bytes.Contains([]byte(errMsg), []byte("exception")) {
		t.Fatalf("error should mention 'exception', got %q", errMsg)
	}
	if !bytes.Contains([]byte(errMsg), []byte("ThrottlingException")) {
		t.Fatalf("error should mention exception type, got %q", errMsg)
	}
	if !bytes.Contains([]byte(errMsg), []byte("rate limited")) {
		t.Fatalf("error should include payload message, got %q", errMsg)
	}
	if onCompleteCalled {
		t.Fatal("OnComplete should not be called on exception frame")
	}
}

func TestParseEventStreamErrorFrame(t *testing.T) {
	payload := []byte(`{"message":"access denied"}`)
	frame := buildEventStreamFrame(t, map[string]string{
		":event-type":   "error",
		":message-type": "error",
		":error-code":   "AccessDeniedException",
	}, payload)

	var onCompleteCalled bool
	callback := &KiroStreamCallback{
		OnComplete: func(inTok, outTok int) {
			onCompleteCalled = true
		},
	}

	err := parseEventStream(bytes.NewReader(frame), callback)
	if err == nil {
		t.Fatal("expected error from error frame, got nil")
	}
	errMsg := err.Error()
	if !bytes.Contains([]byte(errMsg), []byte("error")) {
		t.Fatalf("error should mention 'error', got %q", errMsg)
	}
	if !bytes.Contains([]byte(errMsg), []byte("AccessDeniedException")) {
		t.Fatalf("error should mention error code, got %q", errMsg)
	}
	if !bytes.Contains([]byte(errMsg), []byte("access denied")) {
		t.Fatalf("error should include payload message, got %q", errMsg)
	}
	if onCompleteCalled {
		t.Fatal("OnComplete should not be called on error frame")
	}
}

func TestParseEventStreamEmptyPayloadExceptionFrame(t *testing.T) {
	frame := buildEventStreamFrame(t, map[string]string{
		":message-type":   "exception",
		":exception-type": "ValidationException",
	}, nil)

	var onCompleteCalled bool
	callback := &KiroStreamCallback{
		OnComplete: func(inTok, outTok int) {
			onCompleteCalled = true
		},
	}

	err := parseEventStream(bytes.NewReader(frame), callback)
	if err == nil {
		t.Fatal("expected error from empty-payload exception frame, got nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("ValidationException")) {
		t.Fatalf("error should mention exception type, got %q", err.Error())
	}
	if onCompleteCalled {
		t.Fatal("OnComplete should not be called on empty-payload exception frame")
	}
}

func TestParseEventStreamAssistantEventStillCompletes(t *testing.T) {
	frame := buildEventStreamFrame(t, map[string]string{
		":message-type": "event",
		":event-type":   "assistantResponseEvent",
	}, []byte(`{"content":"hello"}`))

	var gotText string
	var onCompleteCalled bool
	callback := &KiroStreamCallback{
		OnText: func(text string, isThinking bool) {
			gotText += text
		},
		OnComplete: func(inTok, outTok int) {
			onCompleteCalled = true
		},
	}

	if err := parseEventStream(bytes.NewReader(frame), callback); err != nil {
		t.Fatalf("expected normal assistant event to parse successfully, got %v", err)
	}
	if gotText != "hello" {
		t.Fatalf("expected OnText to receive hello, got %q", gotText)
	}
	if !onCompleteCalled {
		t.Fatal("expected OnComplete to be called for normal event")
	}
}

func TestParseEventStreamIncompleteFrame(t *testing.T) {
	// Build a frame that declares a longer total_length but body is truncated.
	payload := []byte(`{"content":"hello"}`)
	headers := map[string]string{
		":event-type": "assistantResponseEvent",
	}

	// Build full frame first, then truncate the body.
	fullFrame := buildEventStreamFrame(t, headers, payload)

	// The prelude is 12 bytes. The remaining = totalLength - 12.
	// We give the prelude intact but only part of the remaining bytes.
	prelude := fullFrame[:12]
	totalLength := int(prelude[0])<<24 | int(prelude[1])<<16 | int(prelude[2])<<8 | int(prelude[3])
	remaining := totalLength - 12

	// Provide only half the remaining bytes.
	incompleteData := make([]byte, 12+remaining/2)
	copy(incompleteData, fullFrame[:12+remaining/2])

	var onCompleteCalled bool
	callback := &KiroStreamCallback{
		OnComplete: func(inTok, outTok int) {
			onCompleteCalled = true
		},
	}

	err := parseEventStream(bytes.NewReader(incompleteData), callback)
	if err == nil {
		t.Fatal("expected error from incomplete frame, got nil")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		// io.ReadFull returns io.ErrUnexpectedEOF when it reads fewer bytes than expected
		t.Fatalf("expected io.ErrUnexpectedEOF or io.EOF, got %v", err)
	}
	if onCompleteCalled {
		t.Fatal("OnComplete should not be called on incomplete frame")
	}
}

// ==================== Helper functions for building EventStream frames ====================

// buildEventStreamHeaderString builds a single AWS Event Stream string header (type 7).
func buildEventStreamHeaderString(name, value string) []byte {
	var buf []byte
	buf = append(buf, byte(len(name)))
	buf = append(buf, []byte(name)...)
	buf = append(buf, 7) // string type
	vLen := uint16(len(value))
	buf = append(buf, byte(vLen>>8), byte(vLen))
	buf = append(buf, []byte(value)...)
	return buf
}

// buildEventStreamFrame builds a complete AWS Event Stream binary message.
// It constructs the prelude, headers, payload, and message CRC.
// For testing, we use CRC32 zeros (4 bytes) as placeholder.
func buildEventStreamFrame(t *testing.T, headers map[string]string, payload []byte) []byte {
	t.Helper()

	// Build headers section
	var headersBuf []byte
	for name, value := range headers {
		headersBuf = append(headersBuf, buildEventStreamHeaderString(name, value)...)
	}

	headersLength := len(headersBuf)
	// total_length = 12 (prelude) + headers_len + payload_len + 4 (message CRC)
	totalLength := 12 + headersLength + len(payload) + 4

	// Build prelude (12 bytes): total_length(4) + headers_length(4) + prelude_crc(4)
	prelude := make([]byte, 12)
	binary.BigEndian.PutUint32(prelude[0:4], uint32(totalLength))
	binary.BigEndian.PutUint32(prelude[4:8], uint32(headersLength))
	// prelude CRC: for testing, compute actual CRC32
	preludeCRC := crc32Hash(prelude[0:8])
	binary.BigEndian.PutUint32(prelude[8:12], preludeCRC)

	// Build message CRC input: prelude + headers + payload
	msgCRCInput := make([]byte, 0, totalLength-4)
	msgCRCInput = append(msgCRCInput, prelude...)
	msgCRCInput = append(msgCRCInput, headersBuf...)
	msgCRCInput = append(msgCRCInput, payload...)
	msgCRC := crc32Hash(msgCRCInput)

	// Assemble full message
	var frame []byte
	frame = append(frame, prelude...)
	frame = append(frame, headersBuf...)
	frame = append(frame, payload...)
	frame = append(frame, make([]byte, 4)...)
	binary.BigEndian.PutUint32(frame[len(frame)-4:], msgCRC)

	return frame
}

// crc32Hash computes CRC32 (IEEE polynomial) matching AWS Event Stream spec.
func crc32Hash(data []byte) uint32 {
	// Use the standard library's CRC32 IEEE table
	// AWS Event Stream uses CRC32 with IEEE polynomial (0xEDB88320 / reflected)
	const polynomial = 0xEDB88320
	var crc uint32 = 0xFFFFFFFF
	for _, b := range data {
		crc ^= uint32(b)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ polynomial
			} else {
				crc >>= 1
			}
		}
	}
	return crc ^ 0xFFFFFFFF
}

func TestSetPayloadProfileArnForAccountUsesAccountArn(t *testing.T) {
	payload := &KiroPayload{ProfileArn: "arn:aws:codewhisperer:profile/stale"}

	setPayloadProfileArnForAccount(payload, &config.Account{ProfileArn: " arn:aws:codewhisperer:profile/current "})
	if payload.ProfileArn != "arn:aws:codewhisperer:profile/current" {
		t.Fatalf("expected current account profile ARN, got %q", payload.ProfileArn)
	}
}

func TestSetPayloadProfileArnForAccountPreservesExplicitPayloadArn(t *testing.T) {
	payload := &KiroPayload{ProfileArn: " arn:aws:codewhisperer:profile/explicit "}

	setPayloadProfileArnForAccount(payload, &config.Account{})
	if payload.ProfileArn != "arn:aws:codewhisperer:profile/explicit" {
		t.Fatalf("expected explicit payload profile ARN to be preserved, got %q", payload.ProfileArn)
	}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("invalid test URL: %v", err)
	}
	return parsed
}

func assertProxyURL(t *testing.T, got *url.URL, want string) {
	t.Helper()
	if got == nil {
		t.Fatalf("expected proxy URL %q, got nil", want)
	}
	if got.String() != want {
		t.Fatalf("expected proxy URL %q, got %q", want, got.String())
	}
}

func awsEventStreamFrame(t *testing.T, eventType string, payload map[string]interface{}) []byte {
	t.Helper()

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	headerValue := []byte(eventType)
	headers := make([]byte, 0, 1+len(":event-type")+1+2+len(headerValue))
	headers = append(headers, byte(len(":event-type")))
	headers = append(headers, []byte(":event-type")...)
	headers = append(headers, byte(7))
	headers = append(headers, byte(len(headerValue)>>8), byte(len(headerValue)))
	headers = append(headers, headerValue...)

	totalLength := 12 + len(headers) + len(payloadBytes) + 4
	frame := make([]byte, 12, totalLength)
	binary.BigEndian.PutUint32(frame[0:4], uint32(totalLength))
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(headers)))
	frame = append(frame, headers...)
	frame = append(frame, payloadBytes...)
	frame = append(frame, 0, 0, 0, 0)
	return frame
}
