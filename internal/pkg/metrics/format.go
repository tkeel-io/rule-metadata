package metrics

import (
	"fmt"

	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
)

const UndefineValue = "undefine"

type metadataFmt struct {
	NodeName string
}

func NewmetadataFmt(name string) *metadataFmt {
	return &metadataFmt{NodeName: name}
}

func (this *metadataFmt) Key() string {
	return fmt.Sprintf("/exporters/metadata/%s", this.NodeName)
}

func (this *metadataFmt) Value(args ...string) string {

	var addr string
	if len(args) == 2 {
		addr = fmt.Sprintf("%s:%s", args[0], args[1])
	}
	log.Info("register prometheus exporter Success.", logf.String("info", addr))
	return fmt.Sprintf("{\"addr\":\"%s\"}", addr)
}
