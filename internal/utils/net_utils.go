package utils

import (
	"errors"
	"net"
	"strings"
)

// 通过网卡名称获取ip
func GetIpByName(interfaceName string) (string, error) {
	i, err := net.InterfaceByName(interfaceName)
	if err != nil {
		return "", err
	}

	addr, err := i.Addrs()
	if err != nil {
		return "", err
	}

	for _, v := range addr {
		tmp := v.(*net.IPNet).IP.To4()
		if tmp == nil {
			continue
		}
		trimSpaceIp := strings.TrimSpace(tmp.String())
		// 空字符串
		if strings.EqualFold(trimSpaceIp, "") {
			continue
		}
		// <nil>
		if strings.EqualFold(trimSpaceIp, "<nil>") {
			continue
		}
		return tmp.String(), nil
	}

	return "", errors.New("no ip by interfaceName(" + interfaceName + ")")
}
