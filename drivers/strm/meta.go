package strm

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	Paths           string `json:"paths" required:"true" type:"text"`
	SiteUrl         string `json:"siteUrl" type:"text" required:"false" help:"The prefix URL of the strm file"`
	FilterFileTypes string `json:"filterFileTypes" type:"text" default:"strm" required:"false" help:"Supports suffix name of strm file"`
	UseSign         bool   `json:"signPath" default:"true" required:"true" help:"sign the path in the strm file"`
	EncodePath      bool   `json:"encodePath" default:"true" required:"true" help:"encode the path in the strm file"`
}

var config = driver.Config{
	Name:        "Strm",
	LocalSort:   true,
	NoCache:     true,
	NoUpload:    true,
	DefaultRoot: "/",
	OnlyLocal:   true,
	OnlyProxy:   true,
}

func init() {
	op.RegisterDriver(func() driver.Driver {
		return &Strm{
			Addition: Addition{
				UseSign:    true,
				EncodePath: true,
			},
		}
	})
}
