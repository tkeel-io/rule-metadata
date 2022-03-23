package dao

import (
	"context"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"os"
	"testing"

	"github.com/tkeel-io/rule-metadata/internal/conf"
)

var (
	dao *Dao
	c   *conf.Config
	ctx = context.Background()
)

func TestMain(m *testing.M) {
	var err error
	initLog()
	confPath := "../../../conf/metadata-local.toml"
	c = conf.LoadTomlFile(confPath)
	dao, err = New(c)
	if err != nil {
		log.Error("load toml file fail",
			logf.Any("confPath", confPath),
			logf.Error(err))
		log.Fatal("connect fail")
	}
	os.Exit(m.Run())
	dao.Close()
}

func initLog() {
	log.InitLogger("IoTHub", "metadata")
}
