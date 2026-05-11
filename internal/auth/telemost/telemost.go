package telemost

import (
	"context"
	"fmt"

	"github.com/openlibrecommunity/olcrtc/internal/auth"
)

// Provider produces Goolom credentials for the Yandex Telemost service.
type Provider struct{}

// Engine reports which engine consumes credentials from this auth provider.
func (Provider) Engine() string { return "goolom" }

// Issue fetches connection info for a Telemost room and returns engine credentials.
//
// cfg.RoomURL must be a Telemost conference URL (e.g.
// https://telemost.yandex.ru/j/<id>). Room creation is not supported by the
// Telemost API; rooms originate in the Yandex UI.
func (Provider) Issue(ctx context.Context, cfg auth.Config) (auth.Credentials, error) {
	if cfg.RoomURL == "" {
		return auth.Credentials{}, auth.ErrRoomIDRequired
	}
	info, err := GetConnectionInfo(ctx, cfg.RoomURL, cfg.Name)
	if err != nil {
		return auth.Credentials{}, fmt.Errorf("get connection info: %w", err)
	}
	return auth.Credentials{
		URL:   info.ClientConfig.MediaServerURL,
		Token: info.PeerID,
		Extra: map[string]string{
			"roomID":           info.RoomID,
			"credentials":      info.Credentials,
			"roomURL":          cfg.RoomURL,
			"telemetryReferer": cfg.RoomURL,
		},
	}, nil
}

func init() { //nolint:gochecknoinits // auth registration is the canonical Go pattern for plugins
	auth.Register("telemost", Provider{})
}
