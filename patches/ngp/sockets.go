package ngp

import (
	"os/exec"
	"regexp"
	"strings"
)

func MakeSocketsMap() (map[string]int, error) {
	result := map[string]int{}
	out, err := exec.Command("ss", "-tunp").Output()
	if err != nil {
		return result, err
	}

	rePid := regexp.MustCompile(`pid=(\d*)`)
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		pid := tryRE(rePid, line)
		result[pid] = result[pid] + 1 // summ TCP and UDP
	}
	return result, nil
}

func tryRE(re *regexp.Regexp, s string) string {
	matches := re.FindAllStringSubmatch(s, -1)
	if len(matches) > 0 && len(matches[0]) > 1 {
		return matches[0][1]
	}
	return ""
}
