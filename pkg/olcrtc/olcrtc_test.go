package olcrtc_test

import (
	"context"
	"errors"
	"testing"

	"github.com/openlibrecommunity/olcrtc/internal/auth"
	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc"
	"github.com/pion/webrtc/v4"
)

const (
	stubToken = "tok"
	stubURL   = "wss://x/"
)

// --- stub engine ---

type stubSession struct{ connected bool }

func (s *stubSession) Connect(_ context.Context) error                  { s.connected = true; return nil }
func (s *stubSession) Send(_ []byte) error                              { return nil }
func (s *stubSession) Close() error                                     { return nil }
func (s *stubSession) SetReconnectCallback(_ func(*webrtc.DataChannel)) {}
func (s *stubSession) SetShouldReconnect(_ func() bool)                 {}
func (s *stubSession) SetEndedCallback(_ func(string))                  {}
func (s *stubSession) WatchConnection(_ context.Context)                {}
func (s *stubSession) CanSend() bool                                    { return s.connected }
func (s *stubSession) GetSendQueue() chan []byte                        { return nil }
func (s *stubSession) GetBufferedAmount() uint64                        { return 0 }
func (s *stubSession) Capabilities() engine.Capabilities               { return engine.Capabilities{ByteStream: true} }

// Compile-time check: stubSession must satisfy engine.Session.
var _ engine.Session = (*stubSession)(nil)

func registerStubEngine(t *testing.T, name string) {
	t.Helper()
	engine.Register(name, func(_ context.Context, _ engine.Config) (engine.Session, error) {
		return &stubSession{}, nil
	})
	t.Cleanup(func() {
		// Re-register a no-op so subsequent tests don't break.
		engine.Register(name, func(_ context.Context, _ engine.Config) (engine.Session, error) {
			return &stubSession{}, nil
		})
	})
}

// --- stub auth ---

type stubAuth struct{ engineName string }

func (a stubAuth) Engine() string { return a.engineName }
func (a stubAuth) Issue(_ context.Context, cfg auth.Config) (auth.Credentials, error) {
	if cfg.RoomURL == "" {
		return auth.Credentials{}, auth.ErrRoomIDRequired
	}
	return auth.Credentials{URL: "wss://stub/", Token: stubToken}, nil
}

type stubAuthWithRoomCreator struct{ stubAuth }

func (stubAuthWithRoomCreator) CreateRoom(_ context.Context, _ auth.Config) (string, error) {
	return "created-room-id", nil
}

func registerStubAuth(t *testing.T, name, engineName string) {
	t.Helper()
	auth.Register(name, stubAuth{engineName: engineName})
}

func registerStubAuthWithCreator(t *testing.T, name, engineName string) {
	t.Helper()
	auth.Register(name, stubAuthWithRoomCreator{stubAuth{engineName: engineName}})
}

// --- tests ---

func TestNewDirect_MissingURL(t *testing.T) {
	_, err := olcrtc.New(context.Background(), olcrtc.Config{Token: "tok"})
	if !errors.Is(err, olcrtc.ErrURLRequired) {
		t.Fatalf("New(no url) = %v, want ErrURLRequired", err)
	}
}

func TestNewDirect_MissingToken(t *testing.T) {
	_, err := olcrtc.New(context.Background(), olcrtc.Config{URL: stubURL})
	if !errors.Is(err, olcrtc.ErrTokenRequired) {
		t.Fatalf("New(no token) = %v, want ErrTokenRequired", err)
	}
}

func TestNewDirect_UnknownEngine(t *testing.T) {
	_, err := olcrtc.New(context.Background(), olcrtc.Config{
		Engine: "no-such-engine",
		URL:    stubURL,
		Token:  stubToken,
	})
	if err == nil {
		t.Fatal("New(bad engine) error = nil")
	}
}

func TestNewDirect_OK(t *testing.T) {
	registerStubEngine(t, "stub-direct")

	sess, err := olcrtc.New(context.Background(), olcrtc.Config{
		Engine: "stub-direct",
		URL:    stubURL,
		Token:  stubToken,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := sess.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if !sess.CanSend() {
		t.Fatal("CanSend() = false after connect")
	}
	if err := sess.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewAuth_UnknownProvider(t *testing.T) {
	_, err := olcrtc.New(context.Background(), olcrtc.Config{
		Auth:   "no-such-auth",
		RoomID: "room",
	})
	if err == nil {
		t.Fatal("New(bad auth) error = nil")
	}
}

func TestNewAuth_MissingRoomID(t *testing.T) {
	registerStubEngine(t, "stub-auth-engine")
	registerStubAuth(t, "stub-auth-noroomid", "stub-auth-engine")

	_, err := olcrtc.New(context.Background(), olcrtc.Config{
		Auth: "stub-auth-noroomid",
		// RoomID intentionally empty
	})
	if err == nil {
		t.Fatal("New(auth, no room) error = nil")
	}
}

func TestNewAuth_OK(t *testing.T) {
	registerStubEngine(t, "stub-auth-ok-engine")
	registerStubAuth(t, "stub-auth-ok", "stub-auth-ok-engine")

	sess, err := olcrtc.New(context.Background(), olcrtc.Config{
		Auth:   "stub-auth-ok",
		RoomID: "some-room",
	})
	if err != nil {
		t.Fatalf("New(auth) error = %v", err)
	}
	if err := sess.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	_ = sess.Close()
}

func TestRegisterDefaults_Idempotent(_ *testing.T) {
	olcrtc.RegisterDefaults()
	olcrtc.RegisterDefaults()
}

func TestCreateRoom_Unsupported(t *testing.T) {
	registerStubAuth(t, "stub-nocreate", "stub-direct")

	_, err := olcrtc.CreateRoom(context.Background(), "stub-nocreate")
	if !errors.Is(err, olcrtc.ErrRoomCreationUnsupported) {
		t.Fatalf("CreateRoom(no creator) = %v, want ErrRoomCreationUnsupported", err)
	}
}

func TestCreateRoom_OK(t *testing.T) {
	registerStubEngine(t, "stub-creator-engine")
	registerStubAuthWithCreator(t, "stub-creator", "stub-creator-engine")

	roomID, err := olcrtc.CreateRoom(context.Background(), "stub-creator")
	if err != nil {
		t.Fatalf("CreateRoom() error = %v", err)
	}
	if roomID == "" {
		t.Fatal("CreateRoom() returned empty room ID")
	}
}

func TestDial_RoundTrip(t *testing.T) {
	registerStubEngine(t, "stub-dial")

	sess, err := olcrtc.New(context.Background(), olcrtc.Config{
		Engine: "stub-dial",
		URL:    stubURL,
		Token:  stubToken,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	c, err := sess.Dial(context.Background())
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}

	// Write should succeed (stub Send is a no-op).
	payload := []byte("hello")
	n, err := c.Write(payload)
	if err != nil || n != len(payload) {
		t.Fatalf("Write() = (%d, %v)", n, err)
	}

	// Close should unblock any pending Read.
	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Read after close should return an error (pipe closed).
	buf := make([]byte, 4)
	_, err = c.Read(buf)
	if err == nil {
		t.Fatal("Read() after Close() should return error")
	}
}
