package config

import "github.com/go-mysql-org/go-mysql/mysql"

type Config struct {
	Host     string
	Port     uint16
	User     string
	Password string
	ServerID uint32
	Flavor   string
	BinlogFile string
	BinlogPos uint32
}

func LoadConfig() Config {
	return Config{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "kodejifr",
		ServerID: 100,
		Flavor:   mysql.MariaDBFlavor,
	}
}

func (c *Config) SetBinlogData(binlogFile string, binlogPos uint32) {
	c.BinlogFile = binlogFile
	c.BinlogPos = binlogPos
}
