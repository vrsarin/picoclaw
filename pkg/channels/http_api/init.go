package http_api

import (
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
)

func init() {
	channels.RegisterFactory(
		config.ChannelHTTPAPI,
		func(channelName, channelType string, cfg *config.Config, b *bus.MessageBus) (channels.Channel, error) {
			bc := cfg.Channels[channelName]
			decoded, err := bc.GetDecoded()
			if err != nil {
				return nil, err
			}
			c, ok := decoded.(*config.HTTPAPISettings)
			if !ok {
				return nil, channels.ErrSendFailed
			}
			modelNames := make([]string, 0, len(cfg.ModelList))
			for _, m := range cfg.ModelList {
				if m.ModelName != "" {
					modelNames = append(modelNames, m.ModelName)
				}
			}
			return NewHTTPAPIChannel(bc, c, b, modelNames)
		},
	)
}
