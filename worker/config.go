package master

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// 程序配置
type Config struct {
	ApiPort         int      `json:"api_port"`
	ApiReadTimeout  int      `json:"api_read_timeout"`
	ApiWriteTimeout int      `json:"api_write_timeout"`
	EtcdEndpoints   []string `json:"etcd_endpoints"`
	EtcdDialTimeout int      `json:"etcd_dial_timeout"`
	WebRoot         string   `json:"web_root"`
}

var (
	// 单例
	G_Conf *Config
)

func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)

	// 1. 把配置文件 读取进来
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	// 2. json反序列化
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}
	// 3. 赋值单例
	G_Conf = &conf
	fmt.Println(conf)

	return
}
