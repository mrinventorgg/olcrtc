// Package olcrtc exposes olcrtc as an embeddable Go library.
//
// Typical usage (direct engine, no service-specific auth):
//
//	sess, err := olcrtc.New(ctx, olcrtc.Config{
//	    Engine: "livekit",
//	    URL:    "wss://sfu.example/",
//	    Token:  "<livekit-jwt>",
//	})
//
// Typical usage (built-in auth provider):
//
//	sess, err := olcrtc.New(ctx, olcrtc.Config{
//	    Auth:   "telemost",
//	    RoomID: "<telemost-room-hash>",
//	})
//
// In both cases the caller must import the engine and (optionally) auth
// packages it needs via blank imports so their init() functions run:
//
//	import (
//	    _ "github.com/openlibrecommunity/olcrtc/internal/engine/livekit"
//	    _ "github.com/openlibrecommunity/olcrtc/internal/auth/telemost"
//	)
//
// Or use [RegisterDefaults] to pull in all built-in implementations at once.
package olcrtc

import (
	"context"
	"errors"
	"fmt"

	"github.com/openlibrecommunity/olcrtc/internal/auth"
	"github.com/openlibrecommunity/olcrtc/internal/carrier/builtin"
	"github.com/openlibrecommunity/olcrtc/internal/engine"
)

var (
	// ErrAuthOrEngineRequired is returned when neither auth nor engine+URL are supplied.
	ErrAuthOrEngineRequired = errors.New("olcrtc: supply either Auth or Engine+URL")
	// ErrURLRequired is returned when direct mode is used without a URL.
	ErrURLRequired = errors.New("olcrtc: URL required when using direct engine mode")
	// ErrTokenRequired is returned when direct mode is used without a token.
	ErrTokenRequired = errors.New("olcrtc: Token required when using direct engine mode")
)

// Config is the input to [New].
type Config struct {
	// --- built-in auth mode ---
	// Auth is the name of a registered auth provider ("telemost", "jazz", "wbstream").
	// When set, RoomID is forwarded to the provider as the room reference.
	Auth   string
	RoomID string

	// --- direct engine mode (Auth == "") ---
	// Engine selects the SFU protocol ("livekit", "goolom", "salutejazz").
	// Defaults to "livekit" when Auth is empty.
	Engine string
	URL    string
	Token  string

	// --- common ---
	// Name is the display name used when joining the room.
	Name string
	// DNSServer is an optional custom DNS resolver (e.g. "1.1.1.1:53").
	DNSServer string
	// ProxyAddr / ProxyPort configure an outbound SOCKS5 proxy.
	ProxyAddr string
	ProxyPort int
	// OnData, when set, receives incoming data-channel bytes. If nil the
	// session operates in video-track / media-only mode.
	OnData func([]byte)
}

// Session is the library handle returned by [New].
// Connect must be called before Send. Close releases all resources.
type Session struct {
	inner engine.Session
	// refresh is stored so it survives reconnects via the engine's Refresh hook.
	authProvider auth.Provider
	authCfg      auth.Config
}

// RegisterDefaults registers all built-in engines and auth providers.
// Call once at program start if you want the full set without manual blank
// imports. Safe to call multiple times.
func RegisterDefaults() {
	builtin.Register()
}

// New creates a Session from cfg. The session is not connected yet; call
// [Session.Connect] when ready.
func New(ctx context.Context, cfg Config) (*Session, error) {
	if cfg.Auth != "" {
		return newWithAuth(ctx, cfg)
	}
	return newDirect(ctx, cfg)
}

func newWithAuth(ctx context.Context, cfg Config) (*Session, error) {
	p, err := auth.Get(cfg.Auth)
	if err != nil {
		return nil, fmt.Errorf("olcrtc: auth provider %q not registered: %w", cfg.Auth, err)
	}

	authCfg := auth.Config{
		RoomURL:   cfg.RoomID,
		Name:      cfg.Name,
		DNSServer: cfg.DNSServer,
		ProxyAddr: cfg.ProxyAddr,
		ProxyPort: cfg.ProxyPort,
	}

	creds, err := p.Issue(ctx, authCfg)
	if err != nil {
		return nil, fmt.Errorf("olcrtc: auth issue: %w", err)
	}

	engineName := p.Engine()
	sess, err := engine.New(ctx, engineName, engine.Config{
		URL:       creds.URL,
		Token:     creds.Token,
		Name:      cfg.Name,
		Extra:     creds.Extra,
		OnData:    cfg.OnData,
		DNSServer: cfg.DNSServer,
		ProxyAddr: cfg.ProxyAddr,
		ProxyPort: cfg.ProxyPort,
		Refresh: func(rCtx context.Context) (engine.Credentials, error) {
			fresh, freshErr := p.Issue(rCtx, authCfg)
			if freshErr != nil {
				return engine.Credentials{}, fmt.Errorf("olcrtc: auth refresh: %w", freshErr)
			}
			return engine.Credentials{URL: fresh.URL, Token: fresh.Token, Extra: fresh.Extra}, nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("olcrtc: engine %q: %w", engineName, err)
	}

	return &Session{inner: sess, authProvider: p, authCfg: authCfg}, nil
}

func newDirect(ctx context.Context, cfg Config) (*Session, error) {
	if cfg.URL == "" {
		return nil, ErrURLRequired
	}
	if cfg.Token == "" {
		return nil, ErrTokenRequired
	}

	engineName := cfg.Engine
	if engineName == "" {
		engineName = "livekit"
	}

	sess, err := engine.New(ctx, engineName, engine.Config{
		URL:       cfg.URL,
		Token:     cfg.Token,
		Name:      cfg.Name,
		OnData:    cfg.OnData,
		DNSServer: cfg.DNSServer,
		ProxyAddr: cfg.ProxyAddr,
		ProxyPort: cfg.ProxyPort,
	})
	if err != nil {
		return nil, fmt.Errorf("olcrtc: engine %q: %w", engineName, err)
	}

	return &Session{inner: sess}, nil
}

// Connect establishes the WebRTC connection. Blocks until the data channel (or
// media) is ready, or ctx is cancelled.
func (s *Session) Connect(ctx context.Context) error {
	if err := s.inner.Connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	return nil
}

// Send queues data for transmission over the data channel.
func (s *Session) Send(data []byte) error {
	if err := s.inner.Send(data); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	return nil
}

// Close tears down the session and releases all resources.
func (s *Session) Close() error {
	if err := s.inner.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return nil
}

// WatchConnection monitors the connection and handles reconnects. Run in a
// goroutine alongside Connect.
func (s *Session) WatchConnection(ctx context.Context) {
	s.inner.WatchConnection(ctx)
}

// CanSend reports whether the session is ready to accept outgoing data.
func (s *Session) CanSend() bool {
	return s.inner.CanSend()
}

// SetEndedCallback registers a function called when the session ends
// permanently (after reconnect exhaustion or explicit close).
func (s *Session) SetEndedCallback(cb func(reason string)) {
	s.inner.SetEndedCallback(cb)
}

// SetShouldReconnect controls whether automatic reconnection is attempted.
func (s *Session) SetShouldReconnect(fn func() bool) {
	s.inner.SetShouldReconnect(fn)
}
