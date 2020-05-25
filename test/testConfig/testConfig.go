package main

import (
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v2"
	"log"
	"os"
)

// 解析后面的名字一定要正确！！！
type Config struct {
	Servers Servers `yaml:"servers"`
}

type Servers struct{ // 转成数组不可以！！！
	RaftServer1 Server `yaml:"server1"`
	RaftServer2 Server `yaml:"server2"`
}

type Server struct{
	Host string `yaml:"host"`
	Name string `yaml:"name"`
}


//read yaml config
//注：path为yaml或yml文件的路径
func ReadYamlConfig(path string) (*Config, error) {
	conf := &Config{}
	if f, err := os.Open(path); err != nil {
		return nil, err
	} else {
		_ = yaml.NewDecoder(f).Decode(conf)
	}
	return conf, nil
}

//test yaml
func main() {
	conf, err := ReadYamlConfig("D:\\go\\tinykv_raft\\test\\testConfig\\test.yaml")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(conf.Servers.RaftServer1.Host)

	byts, err := json.Marshal(conf)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(byts))

}
